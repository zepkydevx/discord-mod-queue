package queue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zepkydevx/discord-mod-queue/internal/ratelimit"
)

// fakeJob lets a test control exactly how a job behaves and count how many
// times it actually ran.
type fakeJob struct {
	name      string
	failTimes int32 // fail this many attempts before succeeding
	attempts  int32
}

func (j *fakeJob) Describe() string { return j.name }

func (j *fakeJob) Execute(ctx context.Context) error {
	n := atomic.AddInt32(&j.attempts, 1)
	if n <= j.failTimes {
		return errors.New("simulated failure")
	}
	return nil
}

func (j *fakeJob) attemptCount() int32 { return atomic.LoadInt32(&j.attempts) }

func unlimitedLimiter() *ratelimit.Limiter {
	return ratelimit.New(1000, 1000) // capacity/refill high enough to never block these tests
}

func fastPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}
}

func TestJobSucceedsOnFirstAttempt(t *testing.T) {
	q := New(2, 10, unlimitedLimiter(), fastPolicy())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q.Start(ctx)

	job := &fakeJob{name: "ok"}
	if err := q.Enqueue(job); err != nil {
		t.Fatalf("unexpected Enqueue error: %v", err)
	}
	q.Stop()

	if job.attemptCount() != 1 {
		t.Fatalf("expected exactly 1 attempt, got %d", job.attemptCount())
	}
}

func TestJobRetriesUntilItSucceeds(t *testing.T) {
	q := New(1, 10, unlimitedLimiter(), fastPolicy())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q.Start(ctx)

	job := &fakeJob{name: "flaky", failTimes: 2} // fails twice, succeeds on the 3rd
	q.Enqueue(job)
	q.Stop()

	if job.attemptCount() != 3 {
		t.Fatalf("expected 3 attempts, got %d", job.attemptCount())
	}
}

func TestJobGivesUpAfterMaxAttempts(t *testing.T) {
	q := New(1, 10, unlimitedLimiter(), fastPolicy())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q.Start(ctx)

	job := &fakeJob{name: "always-fails", failTimes: 100}
	q.Enqueue(job)
	q.Stop()

	if job.attemptCount() != 3 { // the policy's MaxAttempts
		t.Fatalf("expected exactly 3 attempts (the policy max), got %d", job.attemptCount())
	}
}

func TestEnqueueFailsWhenTheBacklogIsFull(t *testing.T) {
	// No workers started, so nothing drains the backlog.
	q := New(1, 1, unlimitedLimiter(), fastPolicy())

	if err := q.Enqueue(&fakeJob{name: "a"}); err != nil {
		t.Fatalf("expected the first job to fit, got %v", err)
	}
	if err := q.Enqueue(&fakeJob{name: "b"}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
}

type blockingJob struct {
	duration time.Duration
	done     *sync.WaitGroup
}

func (j *blockingJob) Describe() string { return "blocking" }

func (j *blockingJob) Execute(ctx context.Context) error {
	time.Sleep(j.duration)
	j.done.Done()
	return nil
}

func TestWorkersProcessJobsConcurrently(t *testing.T) {
	q := New(4, 10, unlimitedLimiter(), fastPolicy())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q.Start(ctx)

	const jobCount = 8
	const jobDuration = 20 * time.Millisecond
	var wg sync.WaitGroup
	wg.Add(jobCount)

	start := time.Now()
	for i := 0; i < jobCount; i++ {
		if err := q.Enqueue(&blockingJob{duration: jobDuration, done: &wg}); err != nil {
			t.Fatalf("unexpected Enqueue error: %v", err)
		}
	}
	waitOrTimeout(t, &wg, 2*time.Second)
	elapsed := time.Since(start)
	q.Stop()

	// 8 jobs at 20ms each across 4 workers is ~2 batches (~40ms). A single
	// worker processing them one at a time would take ~160ms. 120ms leaves
	// comfortable room for scheduler jitter while still catching a
	// regression to serial processing.
	if elapsed > 120*time.Millisecond {
		t.Fatalf("jobs took %v; expected them to run concurrently across workers", elapsed)
	}
}

func TestStopWaitsForInFlightJobsToFinish(t *testing.T) {
	q := New(1, 10, unlimitedLimiter(), fastPolicy())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q.Start(ctx)

	var finished int32
	job := &finishFlagJob{duration: 30 * time.Millisecond, finished: &finished}

	q.Enqueue(job)
	q.Stop() // should block until the job above actually finishes

	if atomic.LoadInt32(&finished) != 1 {
		t.Fatal("expected the in-flight job to have finished before Stop returned")
	}
}

type finishFlagJob struct {
	duration time.Duration
	finished *int32
}

func (j *finishFlagJob) Describe() string { return "finish-flag" }

func (j *finishFlagJob) Execute(ctx context.Context) error {
	time.Sleep(j.duration)
	atomic.StoreInt32(j.finished, 1)
	return nil
}

func TestCancelledContextStopsRetries(t *testing.T) {
	// A deliberately slow policy: long enough that cancelling well before
	// the first retry delay elapses is never a close call, on any runner.
	policy := RetryPolicy{MaxAttempts: 5, BaseDelay: 200 * time.Millisecond, MaxDelay: time.Second}
	q := New(1, 10, unlimitedLimiter(), policy)
	ctx, cancel := context.WithCancel(context.Background())
	q.Start(ctx)

	job := &fakeJob{name: "never-succeeds", failTimes: 1000}
	q.Enqueue(job)

	time.Sleep(20 * time.Millisecond) // let the first attempt happen and fail
	cancel()                          // should interrupt the wait before retry #2
	q.Stop()

	if attempts := job.attemptCount(); attempts != 1 {
		t.Fatalf("expected cancellation to stop retries after 1 attempt, got %d", attempts)
	}
}

func waitOrTimeout(t *testing.T, wg *sync.WaitGroup, timeout time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("timed out waiting for jobs to complete")
	}
}
