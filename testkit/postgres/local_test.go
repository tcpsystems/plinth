package postgres

// startLocalServer starts a throwaway PostgreSQL on a free loopback port
// from the server binaries installed on this machine, or from the testkit
// image when there are none, with the trust authentication method, and
// returns its DSN for "postgres". It is the server the base-DSN tests reuse.
// When neither is available the test is skipped.

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func startLocalServer(t *testing.T) string {
	t.Helper()
	bin, err := postgresBin()
	if err != nil {
		t.Skip(err)
	}
	// The in-image server runs as a container whose data directory is a
	// host path owned by the image's postgres user; killing the podman
	// client stops the container.
	dir := t.TempDir()
	inImage := strings.HasSuffix(bin, imageMark)
	if inImage {
		// A bind source must be a place the container's view and the
		// host's agree on; the test's temporary directory can differ.
		dir = imageHostDir(t)
	}
	data := filepath.Join(dir, "data")
	if inImage {
		// The container cannot mount a path that does not exist on the
		// host, and initdb creates the data directory inside the mount.
		if err := os.MkdirAll(data, 0o755); err != nil {
			t.Fatalf("data directory: %v", err)
		}
	}
	var initdb, srv *exec.Cmd
	if inImage {
		initdb = exec.Command("podman", imageArgs("", "initdb", data,
			"-D", "/data", "-U", "postgres", "-A", "trust", "--no-sync", "-E", "UTF8")...)
	} else {
		initdb = exec.Command(filepath.Join(bin, "initdb"), "-D", data, "-U", "postgres", "-A", "trust", "--no-sync", "-E", "UTF8")
		initdb.Env = serverEnv(bin)
	}
	if out, err := initdb.CombinedOutput(); err != nil {
		t.Fatalf("initdb: %v: %s", err, out)
	}
	port, ok := reservePort()
	if !ok {
		t.Fatal("no free port")
	}
	t.Cleanup(func() { releasePort(port) })
	if inImage {
		// Killing the podman client does not stop the server it started, so
		// the container is named and removed explicitly at cleanup.
		token, err := randomToken()
		if err != nil {
			t.Fatalf("container name: %v", err)
		}
		name := "plinth-testkit-local-" + token
		srv = exec.Command("podman", imageArgs(name, "postgres", data,
			"-D", "/data", "-k", "/tmp", "-p", strconv.Itoa(port), "-c", "listen_addresses=127.0.0.1")...)
		t.Cleanup(func() {
			_ = exec.Command("podman", "rm", "--force", "--time", "0", name).Run()
		})
	} else {
		srv = exec.Command(filepath.Join(bin, "postgres"), "-D", data, "-k", dir,
			"-p", strconv.Itoa(port), "-c", "listen_addresses=127.0.0.1")
		srv.Env = serverEnv(bin)
	}
	srv.Stderr = os.Stderr
	if err := srv.Start(); err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = srv.Process.Kill() }) // the waiter goroutine reaps
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	exited := make(chan struct{})
	go func() { _ = srv.Wait(); close(exited) }()
	if err := awaitServer(ctx, exited, "", "", port); err != nil {
		t.Fatalf("local server: %v", err)
	}
	return "postgres://postgres@127.0.0.1:" + strconv.Itoa(port) + "/postgres?sslmode=disable"
}

func isDir(path string) bool {
	st, err := os.Stat(path)
	return err == nil && st.IsDir()
}

func postgresBin() (string, error) {
	cands := []string{}
	if root := os.Getenv("PGROOT"); root != "" {
		cands = append(cands, filepath.Join(root, "bin"))
	}
	matches, _ := filepath.Glob("/usr/lib/postgresql/*/bin")
	matches2, _ := filepath.Glob("/usr/local/pgsql/bin")
	cands = append(cands, append(matches, matches2...)...)
	for _, dir := range cands {
		if usableBin(dir) {
			return dir + imageMark, nil
		}
	}
	if p, err := exec.LookPath("initdb"); err == nil {
		return filepath.Dir(p), nil
	}
	// No usable host server: run initdb and postgres from the image.
	if _, err := exec.LookPath("podman"); err == nil && imageExists() {
		return imageMark, nil
	}
	return "", fmt.Errorf("no PostgreSQL server binaries for the base-DSN tests (set PGROOT)")
}

// imageMark appended to a bin dir means initdb and postgres run from the
// testkit image in a container (the image is musl-based, so its binaries do
// not run on the host), with dataDir mounted from the host.
const imageMark = "\x00image"

func imageExists() bool {
	return exec.Command("podman", "image", "exists", defaultImage).Run() == nil
}

// imageHostDir returns a temporary directory under the user's home
// directory, removed at cleanup, for data a container mounts from the host.
// Rootless podman here does not see the system temporary directory.
func imageHostDir(t *testing.T) string {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("home directory: %v", err)
	}
	dir, err := os.MkdirTemp(home, ".testkit-postgres-")
	if err != nil {
		t.Fatalf("data directory: %v", err)
	}
	// The container's stat of the bind source follows the parent
	// directory, so it must be traversable by the user it runs as.
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("data directory: %v", err)
	}
	t.Cleanup(func() {
		// The server left it owned by the image's postgres user.
		_ = exec.Command("podman", "run", "--rm", "--network", "none", "--user", "0",
			"--entrypoint", "rm", "--volume", dir+":/d", defaultImage, "-rf", "/d").Run()
		_ = os.RemoveAll(dir)
	})
	return dir
}

// imageArgs returns the container run that executes cmd (initdb or
// postgres) from the testkit image with dataDir as its data directory and
// the host's network namespace, which reaches a loopback server.
func imageArgs(name, cmd, dataDir string, rest ...string) []string {
	// initdb and the server refuse to run as root, so the container starts
	// as root (a podman without user namespaces needs that too), takes
	// ownership of the mounted data directory and drops to the image's
	// postgres user by name, its UID differing by image.
	//
	// The image's entrypoint treats a command named after the server as a
	// request to initialise it, so it is replaced by a shell. With `sh -c
	// SCRIPT NAME ARGS...`, NAME becomes $0 and ARGS become "$@"; passing
	// cmd as NAME leaves "$@" as exactly the arguments the command expects.
	script := "chown -R postgres:postgres /data && exec gosu postgres /usr/local/bin/" + cmd + ` "$@"`
	args := []string{"run", "--rm", "--network", "host", "--user", "0"}
	if name != "" {
		args = append(args, "--name", name)
	}
	args = append(args, "--entrypoint", "sh", "--volume", dataDir+":/data", defaultImage, "-c", script, cmd)
	return append(args, rest...)
}

// usableBin reports whether initdb in dir runs from the host.
func usableBin(dir string) bool {
	cmd := exec.Command(filepath.Join(dir, "initdb"), "--version")
	cmd.Env = serverEnv(dir)
	return cmd.Run() == nil
}

// serverEnv is the environment initdb and postgres run in.
// A bin dir unpacked from a package tree ("<root>/usr/lib/postgresql/X/bin")
// carries its shared libraries next to it, under "<root>/usr/lib"; use
// them when they are there. bin -> X -> postgresql -> lib -> usr.
func serverEnv(dir string) []string {
	env := os.Environ()
	abs, err := filepath.Abs(dir)
	if err != nil {
		return env
	}
	if lib := filepath.Join(abs, "../../../../lib/x86_64-linux-gnu"); isDir(lib) {
		env = append(env, "LD_LIBRARY_PATH="+lib)
	}
	return env
}
