package playground

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestSessionFromRequest(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	if _, err := SessionFromRequest(req); err == nil {
		t.Fatal("missing session accepted")
	}

	req.Header.Set(sessionHeader, "not-a-uuid")
	if _, err := SessionFromRequest(req); err == nil {
		t.Fatal("invalid session accepted")
	}

	id := uuid.NewString()
	req.Header.Set(sessionHeader, id)
	got, err := SessionFromRequest(req)
	if err != nil || got != id {
		t.Fatalf("session = %q, %v; want %q", got, err, id)
	}
}

func TestSessionOwnershipKeys(t *testing.T) {
	session := uuid.NewString()
	other := uuid.NewString()
	key := ownedKey(session)

	if !keyBelongsToSession(&key, session) {
		t.Error("generated key does not belong to its session")
	}
	if keyBelongsToSession(&key, other) {
		t.Error("generated key belongs to another session")
	}
	if keyBelongsToSession(nil, session) {
		t.Error("nil key belongs to a session")
	}
	if !strings.HasPrefix(key, "demo:"+session+":") {
		t.Errorf("key %q lacks expected prefix", key)
	}

	custom, err := customKey(session, "order-1")
	if err != nil {
		t.Fatalf("customKey: %v", err)
	}
	if !keyBelongsToSession(&custom, session) {
		t.Error("custom key does not belong to session")
	}
	if _, err := customKey(session, strings.Repeat("x", 300)); err == nil {
		t.Error("oversized custom key accepted")
	}
}

func TestRequestIP(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "203.0.113.8:4567"
	if got := requestIP(req, false); got != "203.0.113.8" {
		t.Errorf("requestIP = %q", got)
	}
	req.Header.Set("X-Forwarded-For", "198.51.100.2, 10.0.0.1")
	if got := requestIP(req, false); got != "203.0.113.8" {
		t.Errorf("untrusted requestIP = %q", got)
	}
	if got := requestIP(req, true); got != "198.51.100.2" {
		t.Errorf("trusted requestIP = %q", got)
	}

	req.RemoteAddr = "[2001:db8::1]:443"
	if got := requestIP(req, false); got != "2001:db8::1" {
		t.Errorf("ipv6 requestIP = %q", got)
	}
}
