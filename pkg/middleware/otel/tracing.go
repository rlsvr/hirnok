package otel

import (
	"context"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	otelapi "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	hirnok "github.com/rlsvr/hirnok/pkg/nats"
)

const (
	spanNameHandle = "hirnok.handle"

	attrMessagingSystem      = "messaging.system"
	attrMessagingOperation   = "messaging.operation.name"
	attrMessagingDestination = "messaging.destination.name"
	attrMessagingMessageID   = "messaging.message.id"
	attrMessagingBodySize    = "messaging.message.body.size"

	headerNatsMsgID = "Nats-Msg-Id"
)

// Trace wraps a core-NATS handler with an OpenTelemetry span. The
// upstream W3C traceparent/tracestate are read from msg.Header so the
// span continues the caller's trace. The new span is attached to ctx
// and threaded into the handler. Handler errors are recorded on the
// span (RecordError + Error status).
func Trace(h hirnok.Handler, tracer trace.Tracer) hirnok.Handler {
	return func(ctx context.Context, m *natsio.Msg) error {
		var header natsio.Header
		if m != nil {
			header = m.Header
		}

		parent := otelapi.GetTextMapPropagator().Extract(ctx, HeaderCarrier{Header: header})
		spanCtx, span := tracer.Start(parent, spanNameHandle)
		defer span.End()

		subject := ""
		var bodySize int
		if m != nil {
			subject = m.Subject
			bodySize = len(m.Data)
		}
		setMessagingAttrs(span, "process", subject, header.Get(headerNatsMsgID), bodySize)

		err := h(spanCtx, m)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return err
	}
}

// TraceJet is the JetStream equivalent of Trace.
func TraceJet(h hirnok.JetHandler, tracer trace.Tracer) hirnok.JetHandler {
	return func(ctx context.Context, m jetstream.Msg) error {
		var header natsio.Header
		if m != nil {
			header = m.Headers()
		}

		parent := otelapi.GetTextMapPropagator().Extract(ctx, HeaderCarrier{Header: header})
		spanCtx, span := tracer.Start(parent, spanNameHandle)
		defer span.End()

		subject := ""
		var bodySize int
		if m != nil {
			subject = m.Subject()
			bodySize = len(m.Data())
		}
		setMessagingAttrs(span, "process", subject, header.Get(headerNatsMsgID), bodySize)

		err := h(spanCtx, m)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return err
	}
}

// Inject injects OpenTelemetry propagation context from ctx into msg's Header.
// If msg is nil, it does nothing. If msg.Header is nil, it is initialized to an
// empty natsio.Header before injection.
func Inject(ctx context.Context, msg *natsio.Msg) {
	if msg == nil {
		return
	}
	if msg.Header == nil {
		msg.Header = make(natsio.Header)
	}
	otelapi.GetTextMapPropagator().Inject(ctx, HeaderCarrier{Header: msg.Header})
}

// Extract extracts OpenTelemetry propagation context from msg's Header into ctx.
// If msg is nil or msg.Header is nil, ctx is returned unchanged.
func Extract(ctx context.Context, msg *natsio.Msg) context.Context {
	if msg == nil || msg.Header == nil {
		return ctx
	}
	return otelapi.GetTextMapPropagator().Extract(ctx, HeaderCarrier{Header: msg.Header})
}

// HeaderCarrier adapts a nats.Header to OpenTelemetry's TextMapCarrier
// so callers can propagation.Inject / Extract on it. Useful for the
// publish-side tracing snippet documented in the README.
type HeaderCarrier struct {
	Header natsio.Header
}

var _ propagation.TextMapCarrier = HeaderCarrier{}

// Get returns the first value associated with key, or "".
func (c HeaderCarrier) Get(key string) string {
	if c.Header == nil {
		return ""
	}
	return c.Header.Get(key)
}

// Set sets key=value. If the underlying Header map is nil, this is a no-op.
func (c HeaderCarrier) Set(key, value string) {
	if c.Header != nil {
		c.Header.Set(key, value)
	}
}

// Keys returns the header field names.
func (c HeaderCarrier) Keys() []string {
	if c.Header == nil {
		return nil
	}
	keys := make([]string, 0, len(c.Header))
	for k := range c.Header {
		keys = append(keys, k)
	}
	return keys
}

func setMessagingAttrs(span trace.Span, op, subject, msgID string, bodySize int) {
	span.SetAttributes(
		attribute.String(attrMessagingSystem, "nats"),
		attribute.String(attrMessagingOperation, op),
		attribute.String(attrMessagingDestination, subject),
		attribute.String(attrMessagingMessageID, msgID),
		attribute.Int(attrMessagingBodySize, bodySize),
	)
}
