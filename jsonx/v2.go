//go:build goexperiment.jsonv2

package jsonx

import (
	v1 "encoding/json"
	v2 "encoding/json/v2"
)

const engine = "json/v2"

func marshal(v any) ([]byte, error) { return v2.Marshal(v, v1.DefaultOptionsV1()) }

func unmarshal(data []byte, v any) error { return v2.Unmarshal(data, v, v1.DefaultOptionsV1()) }

func unmarshalStrict(data []byte, v any) error {
	return v2.Unmarshal(data, v, v1.DefaultOptionsV1(), v2.RejectUnknownMembers(true))
}
