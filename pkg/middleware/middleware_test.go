package middleware

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	qpnats "github.com/rlsvr/hirnok/pkg/nats"
)

// ---------------- Recover ----------------

func TestRecoverCatchesPanic(t *testing.T) {
	h := Recover(func(_ context.Context, _ int) error {
		panic("boom")
	})
	err := h(context.Background(), 0)
	if err == nil {
		t.Fatal("expected error from panic")
	}
	if !strings.Contains(err.Error(), "panic in handler") || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %q, want panic in handler + boom", err)
	}
}

func TestRecoverCatchesNonStringPanic(t *testing.T) {
	type panicVal struct{ N int }
	h := Recover(func(_ context.Context, _ int) error {
		panic(panicVal{N: 42})
	})
	err := h(context.Background(), 0)
	if err == nil {
		t.Fatal("expected error from panic")
	}
	if !strings.Contains(err.Error(), "42") {
		t.Fatalf("err = %q, want it to contain 42", err)
	}
}

func TestRecoverPassthroughOnSuccess(t *testing.T) {
	h := Recover(func(_ context.Context, _ int) error { return nil })
	if err := h(context.Background(), 0); err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
}

func TestRecoverPassthroughOnError(t *testing.T) {
	sentinel := errors.New("oops")
	h := Recover(func(_ context.Context, _ int) error { return sentinel })
	err := h(context.Background(), 0)
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
}

// ---------------- Retry ----------------

func TestRetrySucceedsAfterFailures(t *testing.T) {
	var calls atomic.Int32
	h := Retry(func(_ context.Context, _ int) error {
		if calls.Add(1) < 3 {
			return errors.New("transient")
		}
		return nil
	}, 3, time.Millisecond, 0, nil)

	if err := h(context.Background(), 0); err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("calls = %d, want 3", got)
	}
}

func TestRetryGivesUpAfterMaxAttempts(t *testing.T) {
	sentinel := errors.New("never works")
	var calls atomic.Int32
	h := Retry(func(_ context.Context, _ int) error {
		calls.Add(1)
		return sentinel
	}, 4, time.Millisecond, 0, nil)

	err := h(context.Background(), 0)
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
	if got := calls.Load(); got != 4 {
		t.Fatalf("calls = %d, want 4", got)
	}
}

func TestRetryAttemptsOneMeansNoRetry(t *testing.T) {
	sentinel := errors.New("nope")
	var calls atomic.Int32
	h := Retry(func(_ context.Context, _ int) error {
		calls.Add(1)
		return sentinel
	}, 1, time.Millisecond, 0, nil)

	err := h(context.Background(), 0)
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
}

func TestRetryShouldRetryFalseStops(t *testing.T) {
	sentinel := errors.New("terminal")
	var calls atomic.Int32
	h := Retry(func(_ context.Context, _ int) error {
		calls.Add(1)
		return sentinel
	}, 5, time.Millisecond, 0, func(err error) bool {
		return !errors.Is(err, sentinel)
	})

	err := h(context.Background(), 0)
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1 (no retries when shouldRetry is false)", got)
	}
}

func TestRetryDefaultIgnoresCtxCanceled(t *testing.T) {
	var calls atomic.Int32
	h := Retry(func(_ context.Context, _ int) error {
		calls.Add(1)
		return context.Canceled
	}, 5, time.Millisecond, 0, nil)

	err := h(context.Background(), 0)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1 (default shouldRetry should not retry ctx errors)", got)
	}
}

func TestRetryDefaultIgnoresDeadlineExceeded(t *testing.T) {
	var calls atomic.Int32
	h := Retry(func(_ context.Context, _ int) error {
		calls.Add(1)
		return context.DeadlineExceeded
	}, 5, time.Millisecond, 0, nil)

	err := h(context.Background(), 0)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
}

func TestRetryDefaultSkipsErrTerminate(t *testing.T) {
	var calls atomic.Int32
	h := Retry(func(_ context.Context, _ int) error {
		calls.Add(1)
		return fmt.Errorf("bad payload: %w", qpnats.ErrTerminate)
	}, 5, time.Millisecond, 0, nil)

	err := h(context.Background(), 0)
	if !errors.Is(err, qpnats.ErrTerminate) {
		t.Fatalf("err = %v, want ErrTerminate", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1 (terminate must short-circuit retry)", got)
	}
}

func TestRetryDefaultSkipsErrSkip(t *testing.T) {
	var calls atomic.Int32
	h := Retry(func(_ context.Context, _ int) error {
		calls.Add(1)
		return qpnats.ErrSkip
	}, 5, time.Millisecond, 0, nil)

	err := h(context.Background(), 0)
	if !errors.Is(err, qpnats.ErrSkip) {
		t.Fatalf("err = %v, want ErrSkip", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
}

func TestRetryRespectsCtxDuringBackoff(t *testing.T) {
	var calls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	h := Retry(func(_ context.Context, _ int) error {
		if calls.Add(1) == 1 {
			cancel() // cancel during the first backoff
		}
		return errors.New("transient")
	}, 5, 50*time.Millisecond, 0, nil)

	start := time.Now()
	err := h(ctx, 0)
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("Retry slept past ctx cancel (took %v)", elapsed)
	}
}

func TestRetryPanicsOnInvalidAttempts(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for attempts <= 0")
		}
	}()
	_ = Retry(func(_ context.Context, _ int) error { return nil }, 0, time.Millisecond, 0, nil)
}

func TestRetryPanicsOnInvalidInitialBackoff(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for initialBackoff <= 0")
		}
	}()
	_ = Retry(func(_ context.Context, _ int) error { return nil }, 3, 0, 0, nil)
}

// ---------------- Composition ----------------

func TestRetryAroundRecover(t *testing.T) {
	// Recover must be *inside* Retry — otherwise the panic bypasses
	// Retry (retry.Do does not catch panics) and only Recover sees it.
	// With Recover inside, the panic becomes an error which Retry then
	// retries.
	var calls atomic.Int32
	h := Retry(Recover(func(_ context.Context, _ int) error {
		if calls.Add(1) == 1 {
			panic("first attempt")
		}
		return nil
	}), 3, time.Millisecond, 0, nil)

	if err := h(context.Background(), 0); err != nil {
		t.Fatalf("err = %v, want nil (panic on attempt 1 should be retried)", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("calls = %d, want 2", got)
	}
}

// ---------------- Integration with pkg/nats types ----------------

func TestRecoverAssignableToNatsHandler(t *testing.T) {
	var coreH qpnats.Handler = func(_ context.Context, _ *natsio.Msg) error { return nil }

	var wrapped qpnats.Handler = Recover(coreH)

	m := &natsio.Msg{Subject: "t", Data: []byte("x")}
	if err := wrapped(context.Background(), m); err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
}

func TestRetryAssignableToJetHandler(t *testing.T) {
	var calls atomic.Int32
	var jetH qpnats.JetHandler = func(_ context.Context, _ jetstream.Msg) error {
		if calls.Add(1) < 2 {
			return errors.New("transient")
		}
		return nil
	}

	var wrapped qpnats.JetHandler = Retry(jetH, 3, time.Millisecond, 0, nil)

	// Verify the assignment compiles AND the wrapper still drives the
	// handler. nil jetstream.Msg is fine — our test handler ignores it.
	if err := wrapped(context.Background(), nil); err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("calls = %d, want 2", got)
	}
}
