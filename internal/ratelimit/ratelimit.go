// Package ratelimit provides a thread-safe token bucket, used to keep the
// worker pool from calling the Discord API faster than it allows.
package ratelimit

import (
	"context"
	"sync"
	"time"
)

// Clock abstracts time so the limiter can be unit tested without real
// waiting. Production code uses systemClock; tests use a fake one.
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// Limiter is a thread-safe token bucket: it starts full, and refills
// gradually over time up to its capacity.
type Limiter struct {
	mu         sync.Mutex
	clock      Clock
	capacity   float64
	tokens     float64
	refillRate float64 // tokens added per second
	lastRefill time.Time
}

// New builds a Limiter allowing up to capacity actions at once, refilling
// at refillPerSecond tokens every second.
func New(capacity, refillPerSecond float64) *Limiter {
	return newWithClock(capacity, refillPerSecond, systemClock{})
}

func newWithClock(capacity, refillPerSecond float64, clock Clock) *Limiter {
	return &Limiter{
		clock:      clock,
		capacity:   capacity,
		tokens:     capacity,
		refillRate: refillPerSecond,
		lastRefill: clock.Now(),
	}
}

func (l *Limiter) refillLocked() {
	now := l.clock.Now()
	elapsed := now.Sub(l.lastRefill).Seconds()
	if elapsed <= 0 {
		return
	}
	l.tokens += elapsed * l.refillRate
	if l.tokens > l.capacity {
		l.tokens = l.capacity
	}
	l.lastRefill = now
}

// Allow reports whether an action may proceed right now, consuming a token
// if so. It never blocks.
func (l *Limiter) Allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.refillLocked()
	if l.tokens >= 1 {
		l.tokens--
		return true
	}
	return false
}

// Wait blocks until a token becomes available, or returns early if ctx is
// cancelled or its deadline passes.
func (l *Limiter) Wait(ctx context.Context) error {
	for {
		if l.Allow() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}
