package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rlsvr/hirnok/pkg/queue"
)

func TestRunPanicsOnZero(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for n <= 0")
		}
	}()
	q := queue.New[int](4)
	_ = Run(context.Background(), 0, q, func(_ context.Context, _ int) {})
}

func TestRunDispatchesAllItems(t *testing.T) {
	q := queue.New[int](8)
	var sum atomic.Int64
	p := Run(context.Background(), 3, q, func(_ context.Context, v int) {
		sum.Add(int64(v))
	})

	for i := 1; i <= 10; i++ {
		if err := q.Push(context.Background(), i); err != nil {
			t.Fatalf("push: %v", err)
		}
	}
	q.Close()

	if err := p.Wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if got := sum.Load(); got != 55 {
		t.Fatalf("sum = %d, want 55", got)
	}
}

func TestStoppedClosesAfterQueueClose(t *testing.T) {
	q := queue.New[int](4)
	p := Run(context.Background(), 2, q, func(_ context.Context, _ int) {})
	q.Close()
	select {
	case <-p.Stopped():
	case <-time.After(time.Second):
		t.Fatal("Stopped did not fire after queue close")
	}
}

func TestStoppedClosesAfterCtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	q := queue.New[int](4)
	p := Run(ctx, 2, q, func(_ context.Context, _ int) {})
	cancel()
	select {
	case <-p.Stopped():
	case <-time.After(time.Second):
		t.Fatal("Stopped did not fire after ctx cancel")
	}
}

func TestWaitRespectsCtx(t *testing.T) {
	q := queue.New[int](4)
	p := Run(context.Background(), 1, q, func(_ context.Context, _ int) {})
	defer q.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := p.Wait(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait err = %v, want DeadlineExceeded", err)
	}
}

func TestWaitObservesInFlightCompletion(t *testing.T) {
	q := queue.New[int](4)
	release := make(chan struct{})
	var processed atomic.Int32
	p := Run(context.Background(), 1, q, func(_ context.Context, _ int) {
		<-release
		processed.Add(1)
	})

	if err := q.Push(context.Background(), 1); err != nil {
		t.Fatalf("push: %v", err)
	}

	q.Close()
	time.Sleep(10 * time.Millisecond)
	select {
	case <-p.Stopped():
		t.Fatal("Stopped fired before in-flight handler returned")
	default:
	}

	close(release)
	if err := p.Wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if processed.Load() != 1 {
		t.Fatalf("processed = %d, want 1", processed.Load())
	}
}
