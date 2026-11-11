package config

import (
	"strings"
	"testing"
)

func TestDSNPrecedence(t *testing.T) {
	env := map[string]string{"PRIMARY": "  postgres:///pilot  ", "SECONDARY": "host=localhost dbname=pilot"}
	get := func(k string) string { return env[k] }
	if got := DSN(get, "fallback", "PRIMARY", "SECONDARY"); got != "postgres:///pilot" {
		t.Fatal(got)
	}
	env["PRIMARY"] = " \t "
	if got := DSN(get, "fallback", "PRIMARY", "SECONDARY"); got != env["SECONDARY"] {
		t.Fatal(got)
	}
	if got := DSN(get, "fallback", "MISSING"); got != "fallback" {
		t.Fatal(got)
	}
}

func TestSecretRef(t *testing.T) {
	for _, value := range []string{"", " \t ", "vault/orders-api\ncredential", "secret\x00ref", "private ref-value"} {
		_, err := SecretRef(func(string) string { return value }, "REF")
		if err == nil {
			t.Fatalf("accepted invalid reference")
		}
		if value != "" && strings.Contains(err.Error(), value) {
			t.Fatal("error exposed value")
		}
	}
	got, err := SecretRef(func(string) string { return " vault/orders-api/token " }, "REF")
	if err != nil || got != "vault/orders-api/token" {
		t.Fatal(got, err)
	}
}
