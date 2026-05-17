package nats

import "errors"

// ErrTerminate signals a handler error that should NOT be retried.
// On JetStream the wrapper acks the message (preventing infinite
// redelivery); the DLQ middleware default treats it as DLQ-eligible.
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
