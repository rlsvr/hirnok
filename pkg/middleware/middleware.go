// Package middleware provides small composable decorators for handler
// functions of shape `func(context.Context, M) error` — including
// hirnok's nats.Handler and nats.JetHandler, but generic over any
// message type M.
//
// The package defines a generic Middleware type and Chain helper for composing
// pipelines cleanly:
//
//	h := middleware.Chain(
//		func(h hirnok.Handler) hirnok.Handler { return middleware.Recover(h) },
//		func(h hirnok.Handler) hirnok.Handler { return middleware.Retry(h, 3, ...) },
//	)(myHandler)
//	conn.Subscribe(ctx, "foo", h)
//
// Recover and Retry are fully generic. OpenTelemetry tracing helpers live in
// pkg/middleware/otel so callers only pull OTel dependencies when they need
// them.
package middleware

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/sethvargo/go-retry"

	hirnok "github.com/rlsvr/hirnok/pkg/nats"
)

// PanicError is returned by Recover when a handler panics. It contains the
// original panic value and the stack trace captured at the point of recovery.
type PanicError struct {
	Value any
	Stack []byte
}

func (p *PanicError) Error() string {
	return fmt.Sprintf("panic in handler: %v\n%s", p.Value, p.Stack)
}

func (p *PanicError) Unwrap() error {
	if err, ok := p.Value.(error); ok {
		return err
	}
	return nil
}

// Recover wraps h so panics in the handler become errors instead of
// crashing the worker goroutine. The panic value and stack trace are
// captured and returned as a *PanicError.
func Recover[M any](h func(context.Context, M) error) func(context.Context, M) error {
	if h == nil {
		panic("middleware: Recover requires a non-nil handler")
	}
	return func(ctx context.Context, m M) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = &PanicError{
					Value: r,
					Stack: debug.Stack(),
				}
			}
		}()
		return h(ctx, m)
	}
}

// Middleware wraps a handler function of type H.
type Middleware[H any] func(H) H

// Chain composes multiple middlewares into a single middleware.
// Execution order is outer-to-inner (left-to-right):
// Chain(m1, m2, m3)(h) produces m1(m2(m3(h))).
func Chain[H any](mws ...Middleware[H]) Middleware[H] {
	return func(final H) H {
		for i := len(mws) - 1; i >= 0; i-- {
			if mws[i] != nil {
				final = mws[i](final)
			}
		}
		return final
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
	if h == nil {
		panic("middleware: Retry requires a non-nil handler")
	}
	if attempts <= 0 {
		panic("middleware: Retry attempts must be > 0")
	}
	if initialBackoff <= 0 {
		panic("middleware: Retry initialBackoff must be > 0")
	}
	if shouldRetry == nil {
		shouldRetry = DefaultShouldRetry
	}

	return func(ctx context.Context, m M) error {
		err := h(ctx, m)
		if err == nil || !shouldRetry(err) || attempts == 1 {
			return err
		}

		b := retry.NewExponential(initialBackoff)
		if maxBackoff > 0 {
			b = retry.WithCappedDuration(maxBackoff, b)
		}
		b = retry.WithJitterPercent(10, b)
		b = retry.WithMaxRetries(uint64(attempts-1), b) //nolint:gosec // attempts > 0 validated above

		var t *time.Timer
		defer func() {
			if t != nil {
				t.Stop()
			}
		}()

		for {
			next, stop := b.Next()
			if stop {
				return err
			}
			if t == nil {
				t = time.NewTimer(next)
			} else {
				t.Reset(next)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t.C:
			}
			err = h(ctx, m)
			if err == nil || !shouldRetry(err) {
				return err
			}
		}
	}
}

// DefaultShouldRetry is hirnok's default retry filter. It retries all errors
// EXCEPT context.Canceled, context.DeadlineExceeded, ErrTerminate, ErrSkip, and NakDelayError.
func DefaultShouldRetry(err error) bool {
	var nd *hirnok.NakDelayError
	if errors.Is(err, hirnok.ErrTerminate) || errors.Is(err, hirnok.ErrSkip) || errors.As(err, &nd) {
		return false
	}
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

// WithNonRetryable returns an error predicate that wraps DefaultShouldRetry and
// additionally marks errors matching any of the provided checks as non-retryable.
func WithNonRetryable(checks ...func(error) bool) func(error) bool {
	return func(err error) bool {
		for _, check := range checks {
			if check != nil && check(err) {
				return false
			}
		}
		return DefaultShouldRetry(err)
	}
}
