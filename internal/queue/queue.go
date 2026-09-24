// Package queue runs moderation actions across a fixed pool of concurrent
// workers, respecting a rate limiter and retrying failed jobs with
// exponential backoff, so a burst of actions (a mass-ban, a raid cleanup)
// never overwhelms Discord's API or gets silently dropped on failure.
package queue

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/zepkydevx/discord-mod-queue/internal/ratelimit"
)

// ErrQueueFull is returned by Enqueue when the backlog has no room left.
var ErrQueueFull = errors.New("queue: no room for another job")

// Job is a unit of moderation work the queue executes.
type Job interface {
	// Execute runs one attempt. A non-nil error triggers a retry, up to
	// the configured RetryPolicy.
	Execute(ctx context.Context) error
	// Describe returns a short, human-readable description for logs.
	Describe() string
}

// RetryPolicy controls how many times a failed job is retried and how long
// to wait between attempts. Delay doubles after each failure, capped at
// MaxDelay.
type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

// DefaultRetryPolicy is a sensible starting point: 3 attempts total, with
// the wait between them doubling from 500ms up to 10s.
var DefaultRetryPolicy = RetryPolicy{
	MaxAttempts: 3,
	BaseDelay:   500 * time.Millisecond,
	MaxDelay:    10 * time.Second,
}

func (p RetryPolicy) delayFor(attempt int) time.Duration {
	delay := p.BaseDelay << uint(attempt-1) // attempt is 1-indexed
	if delay > p.MaxDelay || delay <= 0 {
		return p.MaxDelay
	}
	return delay
}

// Queue runs jobs across worker goroutines.
//
// Scope note: Stop must only be called after nothing is calling Enqueue
// anymore — like most Go channel-based queues, sending on a closed channel
// panics. A production version would guard that with a closed flag; this
// demo keeps the contract explicit instead.
type Queue struct {
	jobs        chan Job
	limiter     *ratelimit.Limiter
	policy      RetryPolicy
	workerCount int
	wg          sync.WaitGroup
}

// New builds a Queue with workerCount concurrent workers, a backlog bounded
// to size, and the rate limiter each worker waits on before every attempt.
func New(workerCount, size int, limiter *ratelimit.Limiter, policy RetryPolicy) *Queue {
	return &Queue{
		jobs:        make(chan Job, size),
		limiter:     limiter,
		policy:      policy,
		workerCount: workerCount,
	}
}

// Enqueue adds a job to the backlog. It never blocks: if the backlog is
// full, it returns ErrQueueFull immediately rather than stalling the
// caller (typically a Discord event handler).
func (q *Queue) Enqueue(job Job) error {
	select {
	case q.jobs <- job:
		return nil
	default:
		return ErrQueueFull
	}
}

// Start launches the worker goroutines. It returns immediately.
func (q *Queue) Start(ctx context.Context) {
	for i := 0; i < q.workerCount; i++ {
		q.wg.Add(1)
		go q.worker(ctx)
	}
}

// Stop closes the backlog to new work and blocks until every worker has
// finished its current job and exited.
func (q *Queue) Stop() {
	close(q.jobs)
	q.wg.Wait()
}

func (q *Queue) worker(ctx context.Context) {
	defer q.wg.Done()
	for job := range q.jobs {
		q.runWithRetries(ctx, job)
	}
}

func (q *Queue) runWithRetries(ctx context.Context, job Job) {
	var lastErr error

	for attempt := 1; attempt <= q.policy.MaxAttempts; attempt++ {
		if err := q.limiter.Wait(ctx); err != nil {
			log.Printf("queue: %q stopped waiting for the rate limit: %v", job.Describe(), err)
			return
		}

		lastErr = job.Execute(ctx)
		if lastErr == nil {
			return
		}
		log.Printf("queue: attempt %d/%d for %q failed: %v", attempt, q.policy.MaxAttempts, job.Describe(), lastErr)

		if attempt == q.policy.MaxAttempts {
			break
		}

		select {
		case <-time.After(q.policy.delayFor(attempt)):
		case <-ctx.Done():
			log.Printf("queue: %q cancelled before its next retry", job.Describe())
			return
		}
	}

	log.Printf("queue: %q failed permanently after %d attempts: %v", job.Describe(), q.policy.MaxAttempts, lastErr)
}
