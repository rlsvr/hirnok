package middleware

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	hirnok "github.com/rlsvr/hirnok/pkg/nats"
)

// Header names attached to DLQ-republished messages so operators can
// see why a payload landed in the dead-letter queue without digging
// into logs.
const (
	HeaderDLQReason    = "Hirnok-DLQ-Reason"
	HeaderDLQSubject   = "Hirnok-DLQ-Subject"
	HeaderDLQAt        = "Hirnok-DLQ-At"
	HeaderDLQMessageID = "Hirnok-DLQ-Message-Id"

	maxDLQReasonLen = 1024
)

// DLQOption configures dead-letter message headers and behavior.
type DLQOption func(*dlqOptions)

type dlqOptions struct {
	staticHeaders map[string]string
	headerFunc    func(h natsio.Header, err error, origSubject string)
}

// WithDLQHeaders adds static extra headers to dead-lettered messages.
func WithDLQHeaders(headers map[string]string) DLQOption {
	return func(o *dlqOptions) {
		if o.staticHeaders == nil {
			o.staticHeaders = make(map[string]string, len(headers))
		}
		for k, v := range headers {
			o.staticHeaders[k] = v
		}
	}
}

// WithDLQHeaderFunc allows custom dynamic header generation on dead-lettered messages.
// It is called after default headers (HeaderDLQReason, etc.) and WithDLQHeaders are applied.
func WithDLQHeaderFunc(fn func(h natsio.Header, err error, origSubject string)) DLQOption {
	return func(o *dlqOptions) {
		o.headerFunc = fn
	}
}

func parseDLQOptions(opts []DLQOption) dlqOptions {
	var o dlqOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o
}

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
// Custom headers can be added via WithDLQHeaders or WithDLQHeaderFunc options.
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
	h hirnok.Handler,
	conn *hirnok.Conn,
	subject string,
	shouldDLQ func(error) bool,
	opts ...DLQOption,
) hirnok.Handler {
	if h == nil {
		panic("middleware: DLQ requires a non-nil handler")
	}
	if conn == nil {
		panic("middleware: DLQ requires a non-nil conn")
	}
	if subject == "" {
		panic("middleware: DLQ requires a non-empty subject")
	}
	if shouldDLQ == nil {
		shouldDLQ = DefaultShouldDLQ
	}
	opt := parseDLQOptions(opts)

	return func(ctx context.Context, m *natsio.Msg) error {
		origSubject := subjectOf(m)
		if origSubject == subject {
			return nil
		}
		err := h(ctx, m)
		if err == nil {
			return nil
		}
		if errors.Is(err, hirnok.ErrSkip) {
			return nil
		}
		if !shouldDLQ(err) {
			return err
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}

		out := &natsio.Msg{
			Subject: subject,
			Data:    dataOf(m),
			Header:  cloneHeader(headerOf(m)),
		}
		applyDLQHeaders(out.Header, err, origSubject, opt)

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
	h hirnok.JetHandler,
	js *hirnok.JetStream,
	subject string,
	shouldDLQ func(error) bool,
	opts ...DLQOption,
) hirnok.JetHandler {
	if h == nil {
		panic("middleware: DLQJet requires a non-nil handler")
	}
	if js == nil {
		panic("middleware: DLQJet requires a non-nil JetStream")
	}
	if subject == "" {
		panic("middleware: DLQJet requires a non-empty subject")
	}
	if shouldDLQ == nil {
		shouldDLQ = DefaultShouldDLQ
	}
	opt := parseDLQOptions(opts)

	return func(ctx context.Context, m jetstream.Msg) error {
		origSubject := jetSubjectOf(m)
		if origSubject == subject {
			return nil
		}
		err := h(ctx, m)
		if err == nil {
			return nil
		}
		if errors.Is(err, hirnok.ErrSkip) {
			return nil
		}
		if !shouldDLQ(err) {
			return err
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}

		out := &natsio.Msg{
			Subject: subject,
			Data:    jetDataOf(m),
			Header:  cloneHeader(jetHeaderOf(m)),
		}
		applyDLQHeaders(out.Header, err, origSubject, opt)

		if _, perr := js.PublishMsg(ctx, out); perr != nil {
			return dlqPublishError(err, subject, perr)
		}
		return nil
	}
}

// DefaultShouldDLQ routes every error to DLQ EXCEPT signals that mean
// "try again later" (ctx errors, NakDelayError) or "intentionally skip" (ErrSkip).
// ErrTerminate is DLQ'd — that's its point.
func DefaultShouldDLQ(err error) bool {
	var nd *hirnok.NakDelayError
	if errors.Is(err, hirnok.ErrSkip) || errors.As(err, &nd) {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return true
}

func dlqPublishError(handlerErr error, subject string, publishErr error) error {
	publishErr = fmt.Errorf("dlq publish %q: %w", subject, publishErr)
	if errors.Is(handlerErr, hirnok.ErrTerminate) {
		// If a DLQ write fails, do not preserve ErrTerminate in the returned
		// error. JetStream dispatch treats ErrTerminate as a Term signal; the
		// right recovery path for failed DLQ writes is redelivery.
		return fmt.Errorf("%v: %w", handlerErr, publishErr) //nolint:errorlint // intentionally not wrapped with %w so ErrTerminate is not preserved
	}
	return errors.Join(handlerErr, publishErr)
}

func applyDLQHeaders(h natsio.Header, err error, origSubject string, opt dlqOptions) {
	reason := err.Error()
	if idx := strings.IndexByte(reason, '\n'); idx != -1 {
		reason = reason[:idx]
	}
	if idx := strings.IndexByte(reason, '\r'); idx != -1 {
		reason = reason[:idx]
	}
	if len(reason) > maxDLQReasonLen {
		reason = reason[:maxDLQReasonLen] + "...(truncated)"
	}
	h.Set(HeaderDLQReason, reason)
	h.Set(HeaderDLQSubject, origSubject)
	h.Set(HeaderDLQAt, time.Now().UTC().Format(time.RFC3339))
	for k, v := range opt.staticHeaders {
		h.Set(k, v)
	}
	if opt.headerFunc != nil {
		opt.headerFunc(h, err, origSubject)
	}
}

func cloneHeader(h natsio.Header) natsio.Header {
	out := make(natsio.Header, len(h)+4)
	for k, v := range h {
		switch k {
		case jetstream.MsgIDHeader:
			if len(v) > 0 && v[0] != "" {
				out.Set(HeaderDLQMessageID, v[0])
			}
			continue
		case jetstream.ExpectedStreamHeader,
			jetstream.ExpectedLastSeqHeader,
			jetstream.ExpectedLastSubjSeqHeader,
			jetstream.ExpectedLastMsgIDHeader:
			continue
		}
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
