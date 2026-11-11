package realtime

import (
	"bytes"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/tcpsystems/plinth/jsonx"
	"github.com/tcpsystems/plinth/problem"
)

// SSE returns the text/event-stream handler for this hub. Each delivered
// Event becomes:
//
//	id: <seq>
//	event: <kind>
//	data: {"seq":…,"topic":…,"kind":…,"data":…}
//
// and fresh subscribers (no Last-Event-ID) additionally get a "retry:" hint
// and, when Options.Snapshot is set, a "snapshot" event whose id is the
// as_of_seq boundary. A reconnect carrying Last-Event-ID resumes from the
// ring; if that cursor is no longer retained the server emits "reset" with
// id set to the current head (reason cursor_out_of_range), a fresh
// "snapshot" if configured, then any deltas still buffered, and closes the
// stream: the browser reconnects with the announced cursor, which is in
// range, so the resume cannot loop. A subscriber that cannot keep up gets
// "reset" (reason backpressure) and the stream is closed, so the browser
// reconnects with its last seen id.
// Comment lines (":hb") are written every Options.Heartbeat to keep proxies
// from timing the connection idle.
func (h *Hub) SSE(o Options) http.Handler {
	o = o.withDefaults()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := originErr(o, r); err != nil {
			problem.WriteError(w, r, err)
			return
		}
		p, err := authenticate(o, r)
		if err != nil {
			problem.WriteError(w, r, err)
			return
		}
		topics, err := parseTopics(r)
		if err != nil {
			problem.WriteError(w, r, err)
			return
		}
		if err := authorizeErr(o, p, topics); err != nil {
			problem.WriteError(w, r, err)
			return
		}
		cursor, _ := strconv.ParseUint(r.Header.Get("Last-Event-ID"), 10, 64)
		flush, ok := w.(http.Flusher)
		if !ok {
			problem.WriteError(w, r, problem.NewStatus(http.StatusInternalServerError, nil, "streaming_unsupported", "server cannot flush"))
			return
		}
		sub := h.Subscribe(topics, cursor)
		defer sub.Cancel()
		gap := closedNow(sub.Reset)

		// Build the snapshot before the response starts so a failure is a
		// problem, not a broken stream. The subscription exists first, so
		// deltas that race the snapshot are buffered and delivered after it;
		// consumers drop seq <= as_of_seq.
		var snap []byte
		var snapID string
		needSnap := o.Snapshot != nil && (cursor == 0 || gap)
		if needSnap {
			b, asOf, err := o.Snapshot(r.Context(), p, topics)
			if err != nil {
				problem.WriteError(w, r, err)
				return
			}
			body, err := jsonx.Marshal(map[string]any{"as_of_seq": asOf, "data": b})
			if err != nil {
				problem.WriteError(w, r, err)
				return
			}
			snap, snapID = body, strconv.FormatUint(asOf, 10)
		}

		hw := w.Header()
		hw.Set("Content-Type", "text/event-stream")
		hw.Set("Cache-Control", "no-cache")
		hw.Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "retry: "+strconv.FormatInt(int64(o.Retry/time.Millisecond), 10)+"\n\n")
		flush.Flush() // commit the headers to the wire: clients wait on them
		if gap {
			// Announce the gap with id = head: the browser's automatic
			// reconnect then resumes from a cursor that is in range, instead
			// of re-triggering the gap on every attempt. The snapshot (when
			// configured) and any deltas that already arrived follow.
			writeSSE(w, flush, strconv.FormatUint(h.Head(), 10), "reset", []byte(`{"reason":"cursor_out_of_range"}`))
			if needSnap {
				writeSSE(w, flush, snapID, "snapshot", snap)
			}
			drainEvents(w, flush, sub)
			return
		}
		if needSnap {
			writeSSE(w, flush, snapID, "snapshot", snap)
		}

		tick := time.NewTicker(o.Heartbeat)
		defer tick.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case ev, open := <-sub.Events:
				if !open {
					return // hub closed mid-stream; client reconnects
				}
				b, err := jsonx.Marshal(ev)
				if err != nil {
					continue // a corrupt event must not kill the stream
				}
				writeSSE(w, flush, strconv.FormatUint(ev.Seq, 10), ev.Kind, b)
			case <-sub.Reset: // overflowed (see hub): flush buffered, announce, close
				drainEvents(w, flush, sub)
				writeSSE(w, flush, "", "reset", []byte(`{"reason":"backpressure"}`))
				return // closing triggers the browser's own reconnect
			case <-tick.C:
				if _, err := io.WriteString(w, ":hb\n\n"); err != nil {
					return
				}
				flush.Flush()
			}
		}
	})
}

// writeSSE writes one event: an optional id, an optional event name and the
// data split over "data:" lines (a line break inside data would otherwise
// end the field, per WHATWG field processing).
func writeSSE(w io.Writer, f http.Flusher, id, event string, data []byte) {
	var b []byte
	if id != "" {
		b = append(b, "id: "...)
		b = append(b, id...)
		b = append(b, '\n')
	}
	if event != "" {
		b = append(b, "event: "...)
		b = append(b, event...)
		b = append(b, '\n')
	}
	for {
		i := bytes.IndexByte(data, '\n')
		if i < 0 {
			break
		}
		b = append(b, "data: "...)
		b = append(b, data[:i]...)
		b = append(b, '\n')
		data = data[i+1:]
	}
	b = append(b, "data: "...)
	b = append(b, data...)
	b = append(b, '\n', '\n')
	if _, err := w.Write(b); err != nil {
		return
	}
	f.Flush()
}

// drainEvents writes everything already buffered on the subscription without
// blocking, so a stream that ends early (gap or backpressure) still delivers
// what made it in. The hub stops sending to a dropped subscription, so the
// buffer only holds what the client had not seen yet.
func drainEvents(w io.Writer, f http.Flusher, sub *Subscription) {
	for {
		select {
		case ev, open := <-sub.Events:
			if !open {
				return
			}
			if b, err := jsonx.Marshal(ev); err == nil {
				writeSSE(w, f, strconv.FormatUint(ev.Seq, 10), ev.Kind, b)
			}
		default:
			return
		}
	}
}

func closedNow(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func originErr(o Options, r *http.Request) error {
	origin := r.Header.Get("Origin")
	if len(o.AllowOrigins) > 0 && origin != "" && !slices.Contains(o.AllowOrigins, origin) {
		return problem.New(problem.ErrForbidden, "origin_not_allowed", "cross-origin requests are not allowed from this origin")
	}
	return nil
}

func authenticate(o Options, r *http.Request) (any, error) {
	if o.Auth == nil {
		return nil, nil
	}
	p, err := o.Auth(r)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func authorizeErr(o Options, p any, topics []string) error {
	for _, t := range topics {
		if !o.Authorize(p, t) {
			return problem.New(problem.ErrForbidden, "topic_forbidden", "not allowed to subscribe to this topic")
		}
	}
	return nil
}

// parseTopics reads ?topics= (repeatable, comma-separated).
func parseTopics(r *http.Request) ([]string, error) {
	var out []string
	for _, v := range r.URL.Query()["topics"] {
		for _, t := range strings.Split(v, ",") {
			if t = strings.TrimSpace(t); t != "" {
				out = append(out, t)
			}
		}
	}
	if len(out) == 0 {
		return nil, problem.New(problem.ErrInvalid, "topics_required", "topics query parameter is required")
	}
	if len(out) > 64 {
		return nil, problem.New(problem.ErrInvalid, "too_many_topics", "too many topics in one request")
	}
	return out, nil
}
