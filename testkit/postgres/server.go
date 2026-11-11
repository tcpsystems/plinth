package postgres

// A container-backed server runs one throwaway container per test process.
// The container shares the host network namespace, so the server listens on
// a loopback port that both the test process and the helper reach directly.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// tcpServer is a server reachable over TCP by the wire client.
type tcpServer struct {
	address string // host:port as written in the DSN
	user    string
	pass    string
	maint   string // maintenance database

	// Set only for a server this process started in a container.
	bin       string // podman binary
	port      int
	container string // container name
}

// The process-wide container. users counts Start calls in progress or
// holding it; the container is removed when the last one ends.
var rec struct {
	mu        sync.Mutex
	srv       *server
	err       error
	starting  chan struct{} // non-nil while a container starts; closed when done
	users     int
	bin       string // podman binary of the container
	port      int
	container string // container name
}

var (
	startMu sync.Mutex // serializes container starts
	ports   = struct {
		sync.Mutex
		inUse map[int]bool
	}{inUse: map[int]bool{}}
)

// containerServer returns the process-wide container-backed server,
// starting its container on first use.
func containerServer(ctx context.Context, opts Options) (*server, error) {
	rec.mu.Lock()
	rec.users++
	for rec.starting != nil {
		ch := rec.starting
		rec.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			rec.mu.Lock()
			rec.users--
			err := ctx.Err()
			last := rec.users == 0 && rec.starting != nil
			rec.mu.Unlock()
			if last {
				collect()
			}
			return nil, err
		}
		rec.mu.Lock()
	}
	if srv := rec.srv; srv != nil {
		rec.mu.Unlock()
		return srv, nil
	}
	rec.starting = make(chan struct{})
	rec.mu.Unlock()

	startMu.Lock()
	srv, err := startContainer(ctx, opts)
	startMu.Unlock()

	rec.mu.Lock()
	rec.srv, rec.err = srv, err
	if srv != nil {
		rec.bin, rec.port, rec.container = srv.tcp.bin, srv.tcp.port, srv.tcp.container
	}
	close(rec.starting)
	rec.starting = nil
	rec.mu.Unlock()
	if err != nil {
		// Waiting users get the error and take nothing; only this one holds.
		rec.mu.Lock()
		rec.users = 1
		rec.mu.Unlock()
		collect()
		return nil, err
	}
	return srv, nil
}

// release gives back one use of the container.
func release() {
	rec.mu.Lock()
	rec.users--
	rec.mu.Unlock()
	collect()
}

// collect removes the container once nothing uses it.
func collect() {
	rec.mu.Lock()
	if rec.users > 0 || rec.starting != nil {
		rec.mu.Unlock()
		return
	}
	bin, port, container, had := rec.bin, rec.port, rec.container, rec.srv != nil
	rec.srv, rec.err, rec.bin, rec.port, rec.container = nil, nil, "", 0, ""
	rec.mu.Unlock()
	if had {
		removeContainer(bin, container)
		releasePort(port)
	}
}

func startContainer(ctx context.Context, opts Options) (*server, error) {
	podman := opts.Podman
	if podman == "" {
		podman = "podman"
	}
	if _, err := exec.LookPath(podman); err != nil {
		return nil, fmt.Errorf("%s not found (%v); set %s to use a running server", podman, err, envTestDSN)
	}
	image := opts.Image
	if image == "" {
		image = defaultImage
	}
	if out, err := exec.CommandContext(ctx, podman, "image", "exists", image).CombinedOutput(); err != nil {
		return nil, fmt.Errorf("image %s is not available: %s (start never pulls)", image, strings.TrimSpace(string(out)))
	}
	token, err := randomToken()
	if err != nil {
		return nil, err
	}
	name := "plinth-testkit-pg-" + token
	port, ok := reservePort()
	if !ok {
		return nil, errors.New("no free loopback port for the container")
	}
	// An empty POSTGRES_HOST_AUTH_METHOD keeps the default authentication
	// (scram-sha-256 for TCP) instead of trust. PGPORT moves the server's
	// listen port; the server is the container's main process.
	run := []string{
		"run", "--interactive", "--network", "host", "--name", name,
		"--pull", "never",
		"--label", "plinth-testkit=postgres",
		"--env", "POSTGRES_PASSWORD=" + token,
		"--env", "POSTGRES_HOST_AUTH_METHOD=",
		"--env", "PGPORT=" + strconv.Itoa(port),
		image,
	}
	cmd := exec.CommandContext(ctx, podman, run...)
	cmd.Stdin = strings.NewReader("") // the container gets no stdin
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	if err := cmd.Start(); err != nil {
		releasePort(port)
		return nil, fmt.Errorf("start container: %w", err)
	}
	// One goroutine reaps the container process; nobody else calls Wait.
	exited := make(chan struct{})
	go func() { _ = cmd.Wait(); close(exited) }()
	err = awaitServer(ctx, exited, podman, name, port)
	if err == nil {
		var srv *server
		srv, err = adoptReady(ctx, tcpServer{address: host(port), user: "postgres", pass: token, maint: "postgres"})
		if err != nil {
			err = fmt.Errorf("connect to container: %w", err)
		} else {
			srv.tcp.bin, srv.tcp.port, srv.tcp.container = podman, port, name
			return srv, nil
		}
	}
	_ = cmd.Process.Kill()
	_ = exec.Command(podman, "rm", "--force", "--time", "0", name).Run()
	<-exited
	releasePort(port)
	return nil, err
}

// adoptReady is adopt, retried while the server is still starting up: its
// port accepts connections before it accepts sessions.
func adoptReady(ctx context.Context, tcp tcpServer) (*server, error) {
	for {
		srv, err := adopt(ctx, tcp)
		if err == nil || !strings.Contains(err.Error(), "57P03") {
			return srv, err
		}
		select {
		case <-ctx.Done():
			return nil, err
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// host is the DSN host for a loopback port. It is "localhost" rather than
// 127.0.0.1 because localhost usually resolves to both 127.0.0.1 and ::1,
// so a driver is served whichever address it picks.
func host(port int) string { return "localhost:" + strconv.Itoa(port) }

// awaitServer returns when the port accepts connections. A container that
// exits first means its server failed to start; the log says why.
func awaitServer(ctx context.Context, exited <-chan struct{}, podman, name string, port int) error {
	for {
		select {
		case <-exited:
			return fmt.Errorf("container exited before its server was ready: %s", containerLog(podman, name))
		case <-ctx.Done():
			return fmt.Errorf("server not ready on port %d: %w", port, ctx.Err())
		default:
		}
		c, err := net.DialTimeout("tcp", host(port), time.Second)
		if err == nil {
			_ = c.Close()
			return nil
		}
		select {
		case <-exited:
			return fmt.Errorf("container exited before its server was ready: %s", containerLog(podman, name))
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// removeContainer removes the one container this process started. It never
// matches by label or prefix: another process running the same tests may have
// its own container alive, and must not lose it.
func removeContainer(podman, name string) {
	if podman == "" {
		podman = "podman"
	}
	if !strings.HasPrefix(name, "plinth-testkit-pg-") {
		return
	}
	_ = exec.Command(podman, "rm", "--force", "--time", "0", name).Run()
}

func containerLog(podman, name string) string {
	out, _ := exec.Command(podman, "logs", name).CombinedOutput()
	s := strings.TrimSpace(string(out))
	if len(s) > 600 {
		s = s[len(s)-600:]
	}
	return s
}

// stop releases the container-backed server. Databases are already dropped.
func (s *server) stop(ctx context.Context) {
	if s.tcp.bin != "" {
		release()
	}
}

// reservePort claims a port free on every loopback address that no other
// server of this process holds. It waits while all that the kernel offers
// are taken.
func reservePort() (int, bool) {
	for try := 0; try < 200; try++ {
		ports.Lock()
		port := freePort(ports.inUse)
		if port != 0 {
			ports.inUse[port] = true
		}
		ports.Unlock()
		if port != 0 {
			return port, true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return 0, false
}

// freePort returns a port free on both 127.0.0.1 and ::1, or 0 when no
// common port was found. Ports in inUse are skipped: a server this process
// started may still be running and listening on only one of the two.
func freePort(inUse map[int]bool) int {
	for try := 0; try < 20; try++ {
		v4, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return 0
		}
		port := v4.Addr().(*net.TCPAddr).Port
		v6, err := net.Listen("tcp", fmt.Sprintf("[::1]:%d", port))
		if err != nil {
			_ = v4.Close()
			continue
		}
		_ = v6.Close()
		_ = v4.Close()
		if !inUse[port] {
			return port
		}
	}
	return 0
}

func releasePort(port int) {
	ports.Lock()
	delete(ports.inUse, port)
	ports.Unlock()
}

// dsn returns the URL handed to tests.
func (s *server) dsn(database string) string {
	userinfo := url.User(s.tcp.user)
	if s.tcp.pass != "" {
		userinfo = url.UserPassword(s.tcp.user, s.tcp.pass)
	}
	u := url.URL{Scheme: "postgres", User: userinfo, Host: s.tcp.address, Path: "/" + database}
	h := s.tcp.address
	if i := strings.LastIndex(h, ":"); i >= 0 {
		h = h[:i]
	}
	q := u.Query()
	if h == "localhost" || h == "127.0.0.1" || h == "::1" || h == "[::1]" {
		// Local test servers: TLS only if the server has it.
		q.Set("sslmode", "prefer")
	}
	u.RawQuery = q.Encode()
	return u.String()
}
