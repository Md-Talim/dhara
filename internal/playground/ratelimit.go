package playground

import (
	"sync"
	"time"
)

// RateLimiter is a small in-memory token bucket, keyed by visitor IP or
// session. It is intentionally per-process: the playground is designed to run
// as a single public instance.
type RateLimiter struct {
	mu       sync.Mutex
	buckets  map[string]*bucket
	rate     float64 // tokens per second
	burst    float64
	now      func() time.Time
	lastGC   time.Time
	gcWindow time.Duration
}

type bucket struct {
	tokens   float64
	last     time.Time
	lastSeen time.Time
}

func NewRateLimiter(perMinute, burst int) *RateLimiter {
	if perMinute < 1 {
		perMinute = 1
	}
	if burst < 1 {
		burst = 1
	}
	return &RateLimiter{
		buckets:  make(map[string]*bucket),
		rate:     float64(perMinute) / 60.0,
		burst:    float64(burst),
		now:      time.Now,
		gcWindow: 10 * time.Minute,
	}
}

// Allow consumes one token for key. When denied it returns the approximate
// duration the caller should wait before retrying.
func (l *RateLimiter) Allow(key string) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	l.gcLocked(now)

	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	}

	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * l.rate
		if b.tokens > l.burst {
			b.tokens = l.burst
		}
		b.last = now
	}
	b.lastSeen = now

	if b.tokens < 1 {
		missing := 1 - b.tokens
		wait := time.Duration(missing / l.rate * float64(time.Second))
		if wait < time.Second {
			wait = time.Second
		}
		return false, wait
	}
	b.tokens--
	return true, 0
}

func (l *RateLimiter) gcLocked(now time.Time) {
	if l.lastGC.IsZero() {
		l.lastGC = now
		return
	}
	if now.Sub(l.lastGC) < l.gcWindow {
		return
	}
	for key, b := range l.buckets {
		if now.Sub(b.lastSeen) > l.gcWindow {
			delete(l.buckets, key)
		}
	}
	l.lastGC = now
}
