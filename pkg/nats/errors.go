package nats

import (
	"errors"
	"fmt"
	"time"
)

// ErrTerminate signals a handler error that should NOT be retried.
// On JetStream the wrapper terminates the message (preventing redelivery
// regardless of MaxDeliver); the DLQ middleware default treats it as
// DLQ-eligible.
// Wrap a domain error to surface a useful reason in DLQ headers:
//
//	return fmt.Errorf("bad payload: %w", nats.ErrTerminate)
var ErrTerminate = errors.New("hirnok: terminate")

// ErrSkip signals a handler should silently ack and drop. No retry,
// no DLQ. Useful when the handler decides the message is irrelevant
// in a way that's distinct from "success".
//
//	return nats.ErrSkip
var ErrSkip = errors.New("hirnok: skip")

// NakDelayError signals that the message should be negatively acknowledged
// with a redelivery delay.
type NakDelayError struct {
	Err   error
	Delay time.Duration
}

func (e *NakDelayError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("hirnok: nak with delay %v", e.Delay)
	}
	return e.Err.Error()
}

func (e *NakDelayError) Unwrap() error {
	return e.Err
}

// NakWithDelay returns an error wrapping err that instructs DispatchJet to
// call m.NakWithDelay(delay) instead of m.Nak().
func NakWithDelay(err error, delay time.Duration) error {
	return &NakDelayError{Err: err, Delay: delay}
}
