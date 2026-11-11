// Package buildinfo reports how a service binary was built.
//
// It reads the VCS stamps the Go toolchain embeds, so services need no
// ldflags: runtime/debug.ReadBuildInfo exposes vcs.revision, vcs.modified and
// vcs.time, plus the toolchain and main module versions.
//
// A build that carries no VCS information (source archive, -buildvcs=false)
// can supply the same values with link-time overrides:
//
//	go build -ldflags "\
//	  -X github.com/tcpsystems/plinth/buildinfo.Revision=abc1234 \
//	  -X github.com/tcpsystems/plinth/buildinfo.Modified=false \
//	  -X github.com/tcpsystems/plinth/buildinfo.BuiltAt=2026-11-10T12:00:00Z"
//
// A non-empty override wins over the embedded value.
package buildinfo

import (
	"encoding/json"
	"net/http"
	"runtime"
	"runtime/debug"
	"time"
)

// Link-time overrides for builds without VCS stamps. Set them with -X; the
// zero value means "use the embedded build information".
var (
	// Revision is a commit sha.
	Revision string
	// Modified is "true" when the work tree was dirty at build time.
	Modified string
	// BuiltAt is the build time in RFC 3339 form.
	BuiltAt string
)

// Unknown is reported as the commit when neither a VCS stamp nor an override
// is available.
const Unknown = "unknown"

// Info describes one build.
type Info struct {
	// Revision is the commit sha, or Unknown.
	Revision string
	// Modified reports whether the work tree was dirty at build time.
	Modified bool
	// Time is the build time; the zero value means unknown.
	Time time.Time
	// GoVersion is the toolchain that built the binary.
	GoVersion string
	// ModuleVersion is the main module version, "(devel)" for local builds.
	ModuleVersion string
}

// Get returns this binary's build information. It never fails: missing values
// are reported as Unknown, false or the zero time.
func Get() Info {
	info := Info{
		Revision:  Revision,
		Modified:  Modified == "true",
		GoVersion: runtime.Version(),
	}
	if t, err := time.Parse(time.RFC3339, BuiltAt); err == nil {
		info.Time = t.UTC()
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		if bi.GoVersion != "" {
			info.GoVersion = bi.GoVersion
		}
		info.ModuleVersion = bi.Main.Version
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				if info.Revision == "" {
					info.Revision = s.Value
				}
			case "vcs.modified":
				if Modified == "" {
					info.Modified = s.Value == "true"
				}
			case "vcs.time":
				if info.Time.IsZero() {
					if t, err := time.Parse(time.RFC3339, s.Value); err == nil {
						info.Time = t.UTC()
					}
				}
			}
		}
	}
	if info.Revision == "" {
		info.Revision = Unknown
	}
	return info
}

// Handler returns the unauthenticated GET /version handler for service. The
// body is {"service","commit","modified","built_at","go"} and is never cached.
func Handler(service string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		info := Get()
		body := struct {
			Service  string `json:"service"`
			Commit   string `json:"commit"`
			Modified bool   `json:"modified"`
			BuiltAt  string `json:"built_at"`
			Go       string `json:"go"`
		}{
			Service:  service,
			Commit:   info.Revision,
			Modified: info.Modified,
			BuiltAt:  formatTime(info.Time),
			Go:       info.GoVersion,
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if r.Method == http.MethodHead {
			return
		}
		_ = json.NewEncoder(w).Encode(body)
	})
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}
