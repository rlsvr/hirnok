package middleware

import (
	"context"
	"errors"
	"fmt"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	qpnats "github.com/rlsvr/hirnok/pkg/nats"
)

// Header names attached to DLQ-republished messages so operators can
// see why a payload landed in the dead-letter queue without digging
// into logs.
const (
	HeaderDLQReason  = "Hirnok-DLQ-Reason"
	HeaderDLQSubject = "Hirnok-DLQ-Subject"
	HeaderDLQAt      = "Hirnok-DLQ-At"
)

// DLQ wraps a core-NATS handler: handler errors matching shouldDLQ are
// republished to subject via conn (with diagnostic headers added), and
// the wrapper returns nil (so the caller acks). Errors that don't match
// shouldDLQ bubble up unchanged. ErrSkip always short-circuits — ack,
// no DLQ publish, return nil.
//
// If shouldDLQ is nil, the default routes every error to DLQ EXCEPT
// ErrSkip, context.Canceled, context.DeadlineExceeded (transient
// "try again later" signals — don't poison the data).
//
// DLQ adds three headers to the republished message (HeaderDLQReason,
// HeaderDLQSubject, HeaderDLQAt). Original headers are preserved.
//
// If the DLQ publish itself fails, DLQ returns the joined original + publish
// error so an outer retry/redelivery path can recover. ErrTerminate is not
// preserved on DLQ publish failure because JetStream dispatch would otherwise
// treat the failure as a Term signal and drop the original message.
//
// Composition:
//   - DLQ INSIDE Retry: each failed attempt is DLQ'd; Retry sees DLQ's
//     nil return and doesn't loop. Use to short-circuit retries for
//     known-unprocessable errors (return ErrTerminate from handler).
//   - DLQ OUTSIDE Retry: only the final post-retry failure is DLQ'd.
//     Use when retries are worth trying first.
//
// Panics if conn is nil or subject is empty.
func DLQ(
	h qpnats.Handler,
	conn *qpnats.Conn,
	subject string,
	shouldDLQ func(error) bool,
) qpnats.Handler {
	if conn == nil {
		panic("middleware: DLQ requires a non-nil conn")
	}
	if subject == "" {
		panic("middleware: DLQ requires a non-empty subject")
	}
	if shouldDLQ == nil {
		shouldDLQ = defaultShouldDLQ
	}

	return func(ctx context.Context, m *natsio.Msg) error {
		err := h(ctx, m)
		if err == nil {
			return nil
		}
		if errors.Is(err, qpnats.ErrSkip) {
			return nil
		}
		if !shouldDLQ(err) {
			return err
		}

		out := &natsio.Msg{
			Subject: subject,
			Data:    dataOf(m),
			Header:  cloneHeader(headerOf(m)),
		}
		addDLQHeaders(out.Header, err, subjectOf(m))

		if perr := conn.PublishMsg(out); perr != nil {
			return dlqPublishError(err, subject, perr)
		}
		return nil
	}
}

// DLQJet is the JetStream equivalent of DLQ; publishes via
// *JetStream.PublishMsg (durable). Same semantics. Panics if js is nil
// or subject is empty.
func DLQJet(
	h qpnats.JetHandler,
	js *qpnats.JetStream,
	subject string,
	shouldDLQ func(error) bool,
) qpnats.JetHandler {
	if js == nil {
		panic("middleware: DLQJet requires a non-nil JetStream")
	}
	if subject == "" {
		panic("middleware: DLQJet requires a non-empty subject")
	}
	if shouldDLQ == nil {
		shouldDLQ = defaultShouldDLQ
	}

	return func(ctx context.Context, m jetstream.Msg) error {
		err := h(ctx, m)
		if err == nil {
			return nil
		}
		if errors.Is(err, qpnats.ErrSkip) {
			return nil
		}
		if !shouldDLQ(err) {
			return err
		}

		out := &natsio.Msg{
			Subject: subject,
			Data:    jetDataOf(m),
			Header:  cloneHeader(jetHeaderOf(m)),
		}
		addDLQHeaders(out.Header, err, jetSubjectOf(m))

		if _, perr := js.PublishMsg(ctx, out); perr != nil {
			return dlqPublishError(err, subject, perr)
		}
		return nil
	}
}

// defaultShouldDLQ routes every error to DLQ EXCEPT signals that mean
// "try again later" (ctx errors) or "intentionally skip" (ErrSkip).
// ErrTerminate is DLQ'd — that's its point.
func defaultShouldDLQ(err error) bool {
	if errors.Is(err, qpnats.ErrSkip) {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return true
}

func dlqPublishError(handlerErr error, subject string, publishErr error) error {
	publishErr = fmt.Errorf("dlq publish %q: %w", subject, publishErr)
	if errors.Is(handlerErr, qpnats.ErrTerminate) {
		// If a DLQ write fails, do not preserve ErrTerminate in the returned
		// error. JetStream dispatch treats ErrTerminate as a Term signal; the
		// right recovery path for failed DLQ writes is redelivery.
		return fmt.Errorf("%v: %w", handlerErr, publishErr)
	}
	return errors.Join(handlerErr, publishErr)
}

func addDLQHeaders(h natsio.Header, err error, origSubject string) {
	h.Set(HeaderDLQReason, err.Error())
	h.Set(HeaderDLQSubject, origSubject)
	h.Set(HeaderDLQAt, time.Now().UTC().Format(time.RFC3339))
}

func cloneHeader(h natsio.Header) natsio.Header {
	out := make(natsio.Header, len(h)+3) // +3 for the DLQ headers we'll add
	for k, v := range h {
		out[k] = append(out[k], v...)
	}
	return out
}

// Helpers to extract fields from messages without requiring the caller to pass
// them separately. They tolerate nil messages because tests sometimes build
// intentionally sparse values.

func dataOf(m *natsio.Msg) []byte {
	if m == nil {
		return nil
	}
	return m.Data
}

func headerOf(m *natsio.Msg) natsio.Header {
	if m == nil {
		return nil
	}
	return m.Header
}

func subjectOf(m *natsio.Msg) string {
	if m == nil {
		return ""
	}
	return m.Subject
}

func jetDataOf(m jetstream.Msg) []byte {
	if m == nil {
		return nil
	}
	return m.Data()
}

func jetHeaderOf(m jetstream.Msg) natsio.Header {
	if m == nil {
		return nil
	}
	return m.Headers()
}

func jetSubjectOf(m jetstream.Msg) string {
	if m == nil {
		return ""
	}
	return m.Subject()
}
