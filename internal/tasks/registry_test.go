package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestRegistry_GetMissingHandler(t *testing.T) {
	r := NewRegistry(nil)

	_, ok := r.Get("does_not_exist")
	if ok {
		t.Error("Get() reported ok=true for an unregistered type")
	}
}

func TestRegistry_AddAndGet(t *testing.T) {
	r := NewRegistry(nil)
	var want HandlerFunc = func(ctx context.Context, payload json.RawMessage) error { return nil }

	r.Add("send_email", want)

	got, ok := r.Get("send_email")
	if !ok {
		t.Fatal("Get() reported ok=false for a registered type")
	}
	if got == nil {
		t.Fatal("Get() returned nil handler for a registered type")
	}
	// Call it to prove it is the same function behaviorally.
	if err := got(context.Background(), nil); err != nil {
		t.Errorf("registered handler returned error: %v", err)
	}
}

func TestRegistry_AddOverwrites(t *testing.T) {
	r := NewRegistry(nil)
	var first HandlerFunc = func(ctx context.Context, payload json.RawMessage) error { return errors.New("first") }
	var second HandlerFunc = func(ctx context.Context, payload json.RawMessage) error { return nil }

	r.Add("t", first)
	r.Add("t", second) // documented: Add overwrites

	got, ok := r.Get("t")
	if !ok {
		t.Fatal("Get() reported ok=false")
	}
	if err := got(context.Background(), nil); err != nil {
		t.Error("Get() returned the old handler; expected the overwriting one")
	}
}

func TestRegistry_InitialMapRespected(t *testing.T) {
	var h HandlerFunc = func(ctx context.Context, payload json.RawMessage) error { return nil }
	r := NewRegistry(map[string]HandlerFunc{"seeded": h})

	if _, ok := r.Get("seeded"); !ok {
		t.Error("Get() could not find a handler passed via the initial map")
	}
}
