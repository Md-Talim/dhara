package store

import (
	"bytes"
	"testing"
)

func TestHashPayload_KeyOrderIndependent(t *testing.T) {
	a := []byte(`{"to":"x@example.com","subject":"hi"}`)
	b := []byte(`{"subject":"hi","to":"x@example.com"}`)

	ha, err := hashPayload(a)
	if err != nil {
		t.Fatalf("hashPayload(a): %v", err)
	}
	hb, err := hashPayload(b)
	if err != nil {
		t.Fatalf("hashPayload(b): %v", err)
	}

	if !bytes.Equal(ha, hb) {
		t.Error("hashes differ for semantically identical payloads with different key order")
	}
}

func TestHashPayload_NestedKeyOrderIndependent(t *testing.T) {
	a := []byte(`{"user":{"id":7,"name":"talim"},"tags":["a","b"]}`)
	b := []byte(`{"tags":["a","b"],"user":{"name":"talim","id":7}}`)

	ha, err := hashPayload(a)
	if err != nil {
		t.Fatalf("hashPayload(a): %v", err)
	}
	hb, err := hashPayload(b)
	if err != nil {
		t.Fatalf("hashPayload(b): %v", err)
	}

	if !bytes.Equal(ha, hb) {
		t.Error("hashes differ for identical nested payloads with different key order")
	}
}

func TestHashPayload_ArrayOrderMatters(t *testing.T) {
	// Arrays are ordered by definition: ["a","b"] != ["b","a"].
	ha, err := hashPayload([]byte(`{"tags":["a","b"]}`))
	if err != nil {
		t.Fatalf("hashPayload(a): %v", err)
	}
	hb, err := hashPayload([]byte(`{"tags":["b","a"]}`))
	if err != nil {
		t.Fatalf("hashPayload(b): %v", err)
	}

	if bytes.Equal(ha, hb) {
		t.Error("array element order must affect the payload hash")
	}
}

func TestHashPayload_WhitespaceIndependent(t *testing.T) {
	a := []byte(`{"a": 1,   "b":2}`)
	b := []byte(`{"a":1,"b":2}`)

	ha, err := hashPayload(a)
	if err != nil {
		t.Fatalf("hashPayload(a): %v", err)
	}
	hb, err := hashPayload(b)
	if err != nil {
		t.Fatalf("hashPayload(b): %v", err)
	}

	if !bytes.Equal(ha, hb) {
		t.Error("hashes differ for payloads differing only in whitespace")
	}
}

func TestHashPayload_DifferentPayloadsDiffer(t *testing.T) {
	ha, err := hashPayload([]byte(`{"a":1}`))
	if err != nil {
		t.Fatalf("hashPayload(a): %v", err)
	}
	hb, err := hashPayload([]byte(`{"a":2}`))
	if err != nil {
		t.Fatalf("hashPayload(b): %v", err)
	}

	if bytes.Equal(ha, hb) {
		t.Error("different payloads produced identical hashes")
	}
}

func TestHashPayload_EmptyObject(t *testing.T) {
	h, err := hashPayload([]byte(`{}`))
	if err != nil {
		t.Fatalf("hashPayload: %v", err)
	}
	if len(h) != 32 { // sha256
		t.Errorf("hash length = %d, want 32", len(h))
	}
}

func TestHashPayload_InvalidJSON(t *testing.T) {
	if _, err := hashPayload([]byte(`{not json`)); err == nil {
		t.Error("expected error for invalid JSON payload, got nil")
	}
}

func TestHashPayload_ScalarPayloads(t *testing.T) {
	// Payloads are not required to be objects; scalars must hash fine too.
	for _, p := range []string{`42`, `"hello"`, `true`, `null`, `[1,2,3]`} {
		if _, err := hashPayload([]byte(p)); err != nil {
			t.Errorf("hashPayload(%s): unexpected error: %v", p, err)
		}
	}
}
