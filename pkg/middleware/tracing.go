package middleware

import (
	"context"

	natsio "github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	qpnats "github.com/rlsvr/hirnok/pkg/nats"
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
func Trace(h qpnats.Handler, tracer trace.Tracer) qpnats.Handler {
	return func(ctx context.Context, m *qpnats.Message) error {
		var header natsio.Header
		if m != nil && m.Msg != nil {
			header = m.Header
		}

		parent := otel.GetTextMapPropagator().Extract(ctx, HeaderCarrier{Header: header})
		spanCtx, span := tracer.Start(parent, spanNameHandle)
		defer span.End()

		subject := ""
		var bodySize int
		if m != nil && m.Msg != nil {
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
func TraceJet(h qpnats.JetHandler, tracer trace.Tracer) qpnats.JetHandler {
	return func(ctx context.Context, m *qpnats.JetMessage) error {
		var header natsio.Header
		if m != nil && m.Msg != nil {
			header = m.Headers()
		}

		parent := otel.GetTextMapPropagator().Extract(ctx, HeaderCarrier{Header: header})
		spanCtx, span := tracer.Start(parent, spanNameHandle)
		defer span.End()

		subject := ""
		var bodySize int
		if m != nil && m.Msg != nil {
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

// Set sets key=value. Panics if the underlying header is nil.
func (c HeaderCarrier) Set(key, value string) {
	c.Header.Set(key, value)
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
