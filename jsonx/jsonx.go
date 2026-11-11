// Package jsonx is the single switch point for JSON encoding in tcpsystems
// services. Services import jsonx instead of encoding/json or
// encoding/json/v2, so the engine can change in one place.
//
// With the jsonv2 experiment on (the default in Go 1.27: internal/buildcfg
// baseline has JSONv2 true) the engine is encoding/json/v2 run with
// encoding/json's DefaultOptionsV1, which keeps v1 output (nil slice is null,
// case-insensitive member match). Build with GOEXPERIMENT=nojsonv2 and the
// same API falls back to encoding/json.
//
// UnmarshalStrict is the mode for new code: unknown members are rejected.
package jsonx

import "io"

// Engine names the active implementation: "json/v2" or "json/v1".
func Engine() string { return engine }

// Marshal encodes v with v1-compatible semantics.
func Marshal(v any) ([]byte, error) {
	return marshal(v)
}

// Unmarshal decodes data into v with v1-compatible semantics.
func Unmarshal(data []byte, v any) error { return unmarshal(data, v) }

// UnmarshalStrict decodes data into v, rejecting unknown object members.
func UnmarshalStrict(data []byte, v any) error { return unmarshalStrict(data, v) }

// Write encodes v followed by a newline to w.
func Write(w io.Writer, v any) error {
	b, err := marshal(v)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = w.Write(b)
	return err
}

// Read decodes one JSON value from r into v; strict as in UnmarshalStrict.
func Read(r io.Reader, v any, strict bool) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if strict {
		return unmarshalStrict(b, v)
	}
	return unmarshal(b, v)
}
