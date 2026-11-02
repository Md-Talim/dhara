package playground

import (
	"testing"
	"time"
)

func TestRateLimiterBurstAndRefill(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	limiter := NewRateLimiter(60, 2)
	limiter.now = func() time.Time { return now }

	if ok, _ := limiter.Allow("a"); !ok {
		t.Fatal("first request denied")
	}
	if ok, _ := limiter.Allow("a"); !ok {
		t.Fatal("second request denied")
	}
	if ok, wait := limiter.Allow("a"); ok {
		t.Fatal("third request allowed despite burst")
	} else if wait <= 0 {
		t.Fatalf("wait = %v, want positive", wait)
	}

	// One token per second at 60/min.
	now = now.Add(time.Second)
	if ok, _ := limiter.Allow("a"); !ok {
		t.Fatal("request denied after refill")
	}

	// A different key has its own bucket.
	if ok, _ := limiter.Allow("b"); !ok {
		t.Fatal("independent key denied")
	}
}

func TestRateLimiterDoesNotExceedBurstAfterIdle(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	limiter := NewRateLimiter(600, 3)
	limiter.now = func() time.Time { return now }

	now = now.Add(time.Hour)
	for i := 0; i < 3; i++ {
		if ok, _ := limiter.Allow("a"); !ok {
			t.Fatalf("request %d denied", i)
		}
	}
	if ok, _ := limiter.Allow("a"); ok {
		t.Fatal("idle refill exceeded burst")
	}
}
