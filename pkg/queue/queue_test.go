package queue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestNewZeroCapacityPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for capacity <= 0")
		}
	}()
	_ = New[int](0)
}

func TestPushPopFIFO(t *testing.T) {
	q := New[int](8)
	ctx := context.Background()
	for i := range 5 {
		if err := q.Push(ctx, i); err != nil {
			t.Fatalf("Push(%d) returned %v", i, err)
		}
	}
	if got := q.Len(); got != 5 {
		t.Fatalf("Len = %d, want 5", got)
	}
	if got := q.Cap(); got != 8 {
		t.Fatalf("Cap = %d, want 8", got)
	}
	for i := range 5 {
		v, err := q.Pop(ctx)
		if err != nil {
			t.Fatalf("Pop returned %v", err)
		}
		if v != i {
			t.Fatalf("Pop = %d, want %d", v, i)
		}
	}
}

func TestTryPushFullReturnsFalse(t *testing.T) {
	q := New[int](2)
	if !q.TryPush(1) {
		t.Fatal("first TryPush should succeed")
	}
	if !q.TryPush(2) {
		t.Fatal("second TryPush should succeed")
	}
	if q.TryPush(3) {
		t.Fatal("third TryPush should fail (queue full)")
	}
}

func TestPushBlocksUntilSpace(t *testing.T) {
	q := New[int](1)
	if !q.TryPush(1) {
		t.Fatal("setup TryPush failed")
	}

	done := make(chan error, 1)
	go func() {
		done <- q.Push(context.Background(), 2)
	}()

	select {
	case <-done:
		t.Fatal("Push returned before queue had space")
	case <-time.After(20 * time.Millisecond):
	}

	if _, err := q.Pop(context.Background()); err != nil {
		t.Fatalf("Pop returned %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Push returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Push did not unblock after Pop")
	}
}

func TestPushRespectsContextCancel(t *testing.T) {
	q := New[int](1)
	if !q.TryPush(1) {
		t.Fatal("setup TryPush failed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := q.Push(ctx, 2)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Push err = %v, want DeadlineExceeded", err)
	}
}

func TestPopBlocksUntilItem(t *testing.T) {
	q := New[int](1)

	type result struct {
		v   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		v, err := q.Pop(context.Background())
		done <- result{v, err}
	}()

	select {
	case <-done:
		t.Fatal("Pop returned before any item was pushed")
	case <-time.After(20 * time.Millisecond):
	}

	if err := q.Push(context.Background(), 42); err != nil {
		t.Fatalf("Push returned %v", err)
	}

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("Pop returned %v", r.err)
		}
		if r.v != 42 {
			t.Fatalf("Pop = %d, want 42", r.v)
		}
	case <-time.After(time.Second):
		t.Fatal("Pop did not unblock after Push")
	}
}

func TestPopRespectsContextCancel(t *testing.T) {
	q := New[int](1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := q.Pop(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Pop err = %v, want DeadlineExceeded", err)
	}
}

func TestCloseDrainsThenErrClosed(t *testing.T) {
	q := New[int](4)
	ctx := context.Background()
	for i := range 3 {
		if err := q.Push(ctx, i); err != nil {
			t.Fatalf("Push(%d) returned %v", i, err)
		}
	}
	q.Close()

	for i := range 3 {
		v, err := q.Pop(ctx)
		if err != nil {
			t.Fatalf("Pop returned %v after Close (item %d)", err, i)
		}
		if v != i {
			t.Fatalf("Pop = %d, want %d", v, i)
		}
	}

	_, err := q.Pop(ctx)
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("Pop after drain err = %v, want ErrClosed", err)
	}

	if err := q.Push(ctx, 99); !errors.Is(err, ErrClosed) {
		t.Fatalf("Push after Close err = %v, want ErrClosed", err)
	}
	if q.TryPush(99) {
		t.Fatal("TryPush after Close should return false")
	}
}

func TestCloseIdempotent(_ *testing.T) {
	q := New[int](1)
	q.Close()
	q.Close()
	q.Close()
}

func TestCloseUnblocksPush(t *testing.T) {
	q := New[int](1)
	q.TryPush(1)

	done := make(chan error, 1)
	go func() {
		done <- q.Push(context.Background(), 2)
	}()

	time.Sleep(20 * time.Millisecond)
	q.Close()

	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Push after Close err = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Push did not unblock after Close")
	}
}

func TestRecvUsableInSelect(t *testing.T) {
	q := New[int](1)
	if err := q.Push(context.Background(), 7); err != nil {
		t.Fatalf("Push returned %v", err)
	}

	select {
	case v := <-q.Recv():
		if v != 7 {
			t.Fatalf("Recv = %d, want 7", v)
		}
	case <-time.After(time.Second):
		t.Fatal("Recv did not yield buffered value")
	}
}

func TestDoneSignalsClose(t *testing.T) {
	q := New[int](1)
	select {
	case <-q.Done():
		t.Fatal("Done fired before Close")
	default:
	}
	q.Close()
	select {
	case <-q.Done():
	case <-time.After(time.Second):
		t.Fatal("Done did not fire after Close")
	}
}

func TestConcurrentProducersConsumers(t *testing.T) {
	const (
		producers     = 8
		consumers     = 4
		perProducer   = 250
		expectedTotal = producers * perProducer
	)
	q := New[int](16)
	ctx := context.Background()

	var producerWG sync.WaitGroup
	producerWG.Add(producers)
	for range producers {
		go func() {
			defer producerWG.Done()
			for i := range perProducer {
				if err := q.Push(ctx, i); err != nil {
					t.Errorf("Push returned %v", err)
					return
				}
			}
		}()
	}

	received := make(chan int, expectedTotal)
	var consumerWG sync.WaitGroup
	consumerWG.Add(consumers)
	for range consumers {
		go func() {
			defer consumerWG.Done()
			for {
				v, err := q.Pop(ctx)
				if errors.Is(err, ErrClosed) {
					return
				}
				if err != nil {
					t.Errorf("Pop returned %v", err)
					return
				}
				received <- v
			}
		}()
	}

	producerWG.Wait()
	for q.Len() > 0 {
		time.Sleep(time.Millisecond)
	}
	q.Close()
	consumerWG.Wait()
	close(received)

	count := 0
	for range received {
		count++
	}
	if count != expectedTotal {
		t.Fatalf("received %d items, want %d", count, expectedTotal)
	}
}
