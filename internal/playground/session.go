package playground

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/google/uuid"
)

const (
	sessionHeader = "X-Playground-Session"
	keyPrefix     = "demo:"
)

var (
	ErrSessionRequired = errors.New("a valid X-Playground-Session header is required")
	ErrNotOwned        = errors.New("task does not belong to this playground session")
)

// SessionFromRequest parses and canonicalizes the browser-generated session
// identifier. It is a correlation identifier only, never authentication.
func SessionFromRequest(r *http.Request) (string, error) {
	raw := strings.TrimSpace(r.Header.Get(sessionHeader))
	if raw == "" {
		return "", ErrSessionRequired
	}
	id, err := uuid.Parse(raw)
	if err != nil {
		return "", ErrSessionRequired
	}
	return id.String(), nil
}

func sessionKey(session, suffix string) string {
	return keyPrefix + session + ":" + suffix
}

func keyBelongsToSession(key *string, session string) bool {
	if key == nil || session == "" {
		return false
	}
	return strings.HasPrefix(*key, keyPrefix+session+":")
}

// ownedKey creates a namespaced idempotency key for a playground-created task.
// The prefix is also what lets a visitor retrieve only their own task payloads.
func ownedKey(session string) string {
	return sessionKey(session, uuid.NewString())
}

func customKey(session, key string) (string, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return "", errors.New("idempotency_key must not be empty")
	}
	full := sessionKey(session, "custom:"+key)
	if len(full) > 255 {
		return "", errors.New("idempotency_key must be 255 characters or fewer after playground namespacing")
	}
	return full, nil
}

func requestIP(r *http.Request, trustProxy bool) string {
	if trustProxy {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			first := strings.TrimSpace(strings.Split(xff, ",")[0])
			if first != "" {
				return first
			}
		}
	}
	host := r.RemoteAddr
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return "unknown"
	}
	return host
}

func requireOwned(taskKey *string, session string) error {
	if !keyBelongsToSession(taskKey, session) {
		return fmt.Errorf("%w", ErrNotOwned)
	}
	return nil
}
