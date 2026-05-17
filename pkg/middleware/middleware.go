// Package middleware provides small composable decorators for handler
// functions of shape `func(context.Context, M) error` — including
// hirnok's nats.Handler and nats.JetHandler, but generic over any
// message type M.
//
// The package intentionally does NOT define a Middleware type or a
// Chain helper. Composition is plain function calls:
//
//	h := middleware.Recover(middleware.Retry(myHandler, 3, ...))
//	conn.Subscribe(ctx, "foo", h)
//
// Recover and Retry are fully generic and have zero dependency on
// pkg/nats. The OpenTelemetry tracing helpers (Trace, TraceJet) live in
// tracing.go and import pkg/nats since they need to read NATS headers.
package middleware

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sethvargo/go-retry"

	qpnats "github.com/rlsvr/hirnok/pkg/nats"
)

// Recover wraps h so panics in the handler become errors instead of
// crashing the worker goroutine. The panic value is formatted with %v
// and surfaced as a plain error.
func Recover[M any](h func(context.Context, M) error) func(context.Context, M) error {
	return func(ctx context.Context, m M) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic in handler: %v", r)
			}
		}()
		return h(ctx, m)
	}
}

// Retry wraps h to retry on error up to `attempts` times with
// exponential backoff (initial → 2× → 4× → ... capped at maxBackoff).
//
// Panics if attempts <= 0 or initialBackoff <= 0. If maxBackoff <= 0
// the backoff is uncapped — exponential growth continues unbounded.
//
// shouldRetry decides which errors are retryable. When nil, the default
// retries every error except context.Canceled / context.DeadlineExceeded.
// Callers supplying their own shouldRetry are responsible for excluding
// ctx errors if that behavior is wanted.
//
// Backoff sleeps respect ctx — if ctx is canceled mid-sleep, Retry
// returns ctx.Err() without further handler calls. On JetStream that
// means an in-flight Retry will exit cleanly when the handler's
// AckWait-derived ctx expires; configure attempts × maxBackoff to stay
// well under AckWait to avoid wasted work.
func Retry[M any](
	h func(context.Context, M) error,
	attempts int,
	initialBackoff, maxBackoff time.Duration,
	shouldRetry func(error) bool,
) func(context.Context, M) error {
	if attempts <= 0 {
		panic("middleware: Retry attempts must be > 0")
	}
	if initialBackoff <= 0 {
		panic("middleware: Retry initialBackoff must be > 0")
	}
	if shouldRetry == nil {
		shouldRetry = defaultShouldRetry
	}

	return func(ctx context.Context, m M) error {
		b := retry.NewExponential(initialBackoff)
		if maxBackoff > 0 {
			b = retry.WithCappedDuration(maxBackoff, b)
		}
		b = retry.WithMaxRetries(uint64(attempts-1), b) //nolint:gosec // attempts > 0 validated above

		return retry.Do(ctx, b, func(ctx context.Context) error {
			err := h(ctx, m)
			if err == nil {
				return nil
			}
			if shouldRetry(err) {
				return retry.RetryableError(err)
			}
			return err
		})
	}
}

func defaultShouldRetry(err error) bool {
	if errors.Is(err, qpnats.ErrTerminate) || errors.Is(err, qpnats.ErrSkip) {
		return false
	}
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}
