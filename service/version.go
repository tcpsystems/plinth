package service

import (
	"runtime"
	"runtime/debug"
)

// Set at link time: -ldflags "-X github.com/tcpsystems/plinth/service.GitSHA=$(git rev-parse HEAD)
// -X github.com/tcpsystems/plinth/service.BuildTime=$(date -u +%FT%TZ)".
// When empty, ReadVersion falls back to the VCS stamps from runtime/debug.
var (
	GitSHA    string
	BuildTime string
)

// Version is the body of GET /version.
type Version struct {
	Service       string            `json:"service"`
	GitSHA        string            `json:"git_sha"`
	BuildTime     string            `json:"build_time"`
	SchemaVersion int               `json:"schema_version"`
	Contracts     map[string]string `json:"contracts"`
	Arch          string            `json:"arch"`
	GoVersion     string            `json:"go_version"`
}

// ReadVersion builds a Version for the named service. schema is the schema
// version this binary expects; contracts maps contract name to version (for
// example "service": "1").
func ReadVersion(name string, schema int, contracts map[string]string) Version {
	v := Version{Service: name, GitSHA: GitSHA, BuildTime: BuildTime, SchemaVersion: schema,
		Contracts: contracts, Arch: runtime.GOARCH, GoVersion: runtime.Version()}
	if v.Contracts == nil {
		v.Contracts = map[string]string{}
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			switch {
			case s.Key == "vcs.revision" && v.GitSHA == "":
				v.GitSHA = s.Value
			case s.Key == "vcs.time" && v.BuildTime == "":
				v.BuildTime = s.Value
			}
		}
	}
	if v.GitSHA == "" {
		v.GitSHA = "unknown"
	}
	if v.BuildTime == "" {
		v.BuildTime = "unknown"
	}
	return v
}
