//go:build !goexperiment.jsonv2

package jsonx

import (
	"bytes"
	"encoding/json"
)

const engine = "json/v1"

func marshal(v any) ([]byte, error) { return json.Marshal(v) }

func unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

func unmarshalStrict(data []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	return d.Decode(v)
}
