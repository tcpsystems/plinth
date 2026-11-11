package realtime

import (
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// Register mounts the whole realtime surface on a mux with one call:
//
//	GET  {prefix}/stream          SSE
//	GET  {prefix}/ws              WebSocket
//	POST {prefix}/rpc/{spec...}   Connect-JSON unary; both methods take
//	GET  {prefix}/rpc/{spec...}     "pkg.Service/Method" as spec
//	GET  {prefix}/schema          served-schema index
//
// rpc and schemas may be nil to skip those surfaces. The hub carries the
// event fabric; o configures auth and the transport tunables. Patterns use
// Go 1.22 ServeMux method+wildcard syntax, so the service's mux must not
// already own these paths (a clash panics, which is a startup bug worth
// panicking on).
func Register(mux *http.ServeMux, prefix string, h *Hub, o Options, rpc *RPC, schemas *Schemas) error {
	if h == nil {
		return errors.New("realtime: Register needs a hub")
	}
	if prefix == "" || prefix[0] != '/' || strings.HasSuffix(prefix, "/") {
		return errors.New(`realtime: Register prefix must start with "/" and have no trailing slash, got ` + strconv.Quote(prefix))
	}
	if strings.ContainsAny(prefix, "*{}") {
		return errors.New("realtime: Register prefix must be a plain path, got " + strconv.Quote(prefix))
	}
	mux.Handle("GET "+prefix+"/stream", h.SSE(o))
	mux.Handle("GET "+prefix+"/ws", h.WS(o))
	if rpc != nil {
		hh := rpc.Handler(o)
		mux.Handle("POST "+prefix+"/rpc/{spec...}", hh)
		mux.Handle("GET "+prefix+"/rpc/{spec...}", hh)
		if schemas != nil {
			schemas.SetRPCMethods(rpc.MethodNames())
		}
	}
	if schemas != nil {
		mux.Handle("GET "+prefix+"/schema", schemas.Handler())
	}
	return nil
}

// MethodNames returns the registered fully-qualified method names, sorted,
// for the schema index.
func (r *RPC) MethodNames() []string {
	out := make([]string, 0, len(r.methods))
	for name := range r.methods {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
