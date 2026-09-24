package ratelimit

import (
	"context"
	"testing"
	"time"
)

type fakeClock struct {
	now time.Time
}

func (c *fakeClock) Now() time.Time { return c.now }

func (c *fakeClock) advance(d time.Duration) { c.now = c.now.Add(d) }

func TestAllowConsumesATokenWhenAvailable(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	limiter := newWithClock(2, 1, clock)

	if !limiter.Allow() {
		t.Fatal("expected the first call to be allowed")
	}
	if !limiter.Allow() {
		t.Fatal("expected the second call to be allowed (capacity is 2)")
	}
}

func TestAllowDeniesOnceTheBucketIsEmpty(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	limiter := newWithClock(1, 1, clock)

	limiter.Allow() // consumes the only token
	if limiter.Allow() {
		t.Fatal("expected the bucket to be empty")
	}
}

func TestTokensRefillOverTime(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	limiter := newWithClock(1, 1, clock) // 1 token per second

	limiter.Allow() // bucket now empty
	clock.advance(1 * time.Second)

	if !limiter.Allow() {
		t.Fatal("expected a token to have refilled after 1 second")
	}
}

func TestTokensNeverExceedCapacity(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	limiter := newWithClock(2, 100, clock) // fast refill

	clock.advance(10 * time.Second) // would overfill past capacity if uncapped

	allowed := 0
	for i := 0; i < 5; i++ {
		if limiter.Allow() {
			allowed++
		}
	}
	if allowed != 2 {
		t.Fatalf("expected exactly 2 allowed calls (the capacity), got %d", allowed)
	}
}

func TestWaitReturnsOnceATokenIsAvailable(t *testing.T) {
	limiter := New(1, 1000) // real clock, fast refill so the test stays quick
	limiter.Allow()         // consume the only token

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	if err := limiter.Wait(ctx); err != nil {
		t.Fatalf("expected Wait to succeed, got %v", err)
	}
}

func TestWaitRespectsContextCancellation(t *testing.T) {
	limiter := New(1, 0.001) // effectively never refills within the test
	limiter.Allow()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := limiter.Wait(ctx); err == nil {
		t.Fatal("expected Wait to return an error once the context is cancelled")
	}
}
