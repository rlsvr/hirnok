package middleware

import (
	"context"
	"errors"
	"testing"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	qpnats "github.com/rlsvr/hirnok/pkg/nats"
)

// ---------------- test setup ----------------

func newRecorder() (*tracetest.SpanRecorder, trace.Tracer) {
	r := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(r))
	return r, tp.Tracer("hirnok-test")
}

// TestMain registers a W3C TraceContext propagator so Inject/Extract
// can round-trip via NATS headers.
func TestMain(m *testing.M) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	m.Run()
}

// stubJSMsg implements jetstream.Msg via embedding (other methods
// inherit the nil interface and panic if called — we never call them).
type stubJSMsg struct {
	jetstream.Msg
	data    []byte
	headers natsio.Header
	subject string
}

func (s *stubJSMsg) Data() []byte           { return s.data }
func (s *stubJSMsg) Headers() natsio.Header { return s.headers }
func (s *stubJSMsg) Subject() string        { return s.subject }

// ---------------- Trace ----------------

func TestTraceStartsHandleSpan(t *testing.T) {
	r, tracer := newRecorder()
	h := Trace(func(_ context.Context, _ *qpnats.Message) error { return nil }, tracer)

	m := &qpnats.Message{Msg: &natsio.Msg{Subject: "foo", Data: []byte("body"), Header: natsio.Header{}}}
	if err := h(context.Background(), m); err != nil {
		t.Fatalf("err = %v", err)
	}

	spans := r.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(spans))
	}
	if spans[0].Name() != spanNameHandle {
		t.Fatalf("span name = %q, want hirnok.handle", spans[0].Name())
	}
	if got := spans[0].Status().Code; got != codes.Unset && got != codes.Ok {
		t.Fatalf("span status = %v, want Unset/Ok on success", got)
	}
}

func TestTraceRecordsErrorOnHandlerError(t *testing.T) {
	r, tracer := newRecorder()
	sentinel := errors.New("nope")
	h := Trace(func(_ context.Context, _ *qpnats.Message) error { return sentinel }, tracer)

	m := &qpnats.Message{Msg: &natsio.Msg{Subject: "foo", Data: []byte("b"), Header: natsio.Header{}}}
	if err := h(context.Background(), m); !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}

	spans := r.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(spans))
	}
	if spans[0].Status().Code != codes.Error {
		t.Fatalf("span status = %v, want Error", spans[0].Status().Code)
	}
	events := spans[0].Events()
	foundErr := false
	for _, e := range events {
		if e.Name == "exception" {
			foundErr = true
			break
		}
	}
	if !foundErr {
		t.Fatal("expected exception event recorded on span")
	}
}

func TestTraceExtractsParent(t *testing.T) {
	r, tracer := newRecorder()

	// Start a parent span outside the wrapped handler.
	parentCtx, parentSpan := tracer.Start(context.Background(), "parent")
	parentTraceID := parentSpan.SpanContext().TraceID()
	parentSpanID := parentSpan.SpanContext().SpanID()

	// Inject the parent into headers as a caller in another service would.
	header := natsio.Header{}
	otel.GetTextMapPropagator().Inject(parentCtx, HeaderCarrier{Header: header})
	parentSpan.End()

	h := Trace(func(_ context.Context, _ *qpnats.Message) error { return nil }, tracer)
	m := &qpnats.Message{Msg: &natsio.Msg{Subject: "foo", Data: []byte("b"), Header: header}}
	if err := h(context.Background(), m); err != nil {
		t.Fatalf("err = %v", err)
	}

	spans := r.Ended()
	// We expect at least 2 spans: parent + handle. Find the handle one.
	var handle sdktrace.ReadOnlySpan
	for _, s := range spans {
		if s.Name() == spanNameHandle {
			handle = s
			break
		}
	}
	if handle == nil {
		t.Fatal("hirnok.handle span not recorded")
	}
	if handle.SpanContext().TraceID() != parentTraceID {
		t.Fatalf("trace IDs do not match: handle=%v parent=%v", handle.SpanContext().TraceID(), parentTraceID)
	}
	if handle.Parent().SpanID() != parentSpanID {
		t.Fatalf("parent span ID mismatch: got=%v want=%v", handle.Parent().SpanID(), parentSpanID)
	}
}

func TestTraceAttributes(t *testing.T) {
	r, tracer := newRecorder()
	h := Trace(func(_ context.Context, _ *qpnats.Message) error { return nil }, tracer)

	header := natsio.Header{}
	header.Set("Nats-Msg-Id", "msg-42")
	m := &qpnats.Message{Msg: &natsio.Msg{Subject: "events.user.signup", Data: []byte("hello"), Header: header}}
	if err := h(context.Background(), m); err != nil {
		t.Fatalf("err = %v", err)
	}

	spans := r.Ended()
	attrs := attrsMap(spans[0].Attributes())

	want := map[string]any{
		"messaging.system":            "nats",
		"messaging.operation.name":    "process",
		"messaging.destination.name":  "events.user.signup",
		"messaging.message.id":        "msg-42",
		"messaging.message.body.size": int64(5),
	}
	for k, v := range want {
		got, ok := attrs[k]
		if !ok {
			t.Errorf("missing attribute %q", k)
			continue
		}
		if got != v {
			t.Errorf("attribute %q = %v, want %v", k, got, v)
		}
	}
}

func TestTracePropagatesCtx(t *testing.T) {
	r, tracer := newRecorder()
	h := Trace(func(ctx context.Context, _ *qpnats.Message) error {
		// Handler should see the active span via ctx.
		span := trace.SpanFromContext(ctx)
		span.SetAttributes(attribute.String("handler.stamp", "yes"))
		return nil
	}, tracer)

	m := &qpnats.Message{Msg: &natsio.Msg{Subject: "x", Data: []byte("b"), Header: natsio.Header{}}}
	if err := h(context.Background(), m); err != nil {
		t.Fatalf("err = %v", err)
	}

	spans := r.Ended()
	attrs := attrsMap(spans[0].Attributes())
	if v, ok := attrs["handler.stamp"]; !ok || v != "yes" {
		t.Fatalf("handler.stamp = %v, want yes (proves ctx threading)", v)
	}
}

// ---------------- TraceJet ----------------

func TestTraceJetEquivalent(t *testing.T) {
	r, tracer := newRecorder()

	// Parent span + propagation through headers.
	parentCtx, parentSpan := tracer.Start(context.Background(), "parent")
	parentTraceID := parentSpan.SpanContext().TraceID()
	header := natsio.Header{}
	otel.GetTextMapPropagator().Inject(parentCtx, HeaderCarrier{Header: header})
	header.Set("Nats-Msg-Id", "js-7")
	parentSpan.End()

	sentinel := errors.New("nope from js")
	h := TraceJet(func(_ context.Context, _ *qpnats.JetMessage) error { return sentinel }, tracer)

	jm := &qpnats.JetMessage{Msg: &stubJSMsg{
		subject: "orders.new",
		data:    []byte("payload"),
		headers: header,
	}}

	if err := h(context.Background(), jm); !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}

	spans := r.Ended()
	var handle sdktrace.ReadOnlySpan
	for _, s := range spans {
		if s.Name() == spanNameHandle {
			handle = s
			break
		}
	}
	if handle == nil {
		t.Fatal("hirnok.handle span not recorded")
	}
	if handle.SpanContext().TraceID() != parentTraceID {
		t.Fatalf("trace IDs do not match")
	}
	if handle.Status().Code != codes.Error {
		t.Fatalf("status = %v, want Error", handle.Status().Code)
	}
	attrs := attrsMap(handle.Attributes())
	if attrs["messaging.destination.name"] != "orders.new" {
		t.Fatalf("destination = %v, want orders.new", attrs["messaging.destination.name"])
	}
	if attrs["messaging.message.id"] != "js-7" {
		t.Fatalf("message.id = %v, want js-7", attrs["messaging.message.id"])
	}
	if attrs["messaging.message.body.size"] != int64(7) {
		t.Fatalf("body.size = %v, want 7", attrs["messaging.message.body.size"])
	}
}

// ---------------- helpers ----------------

func attrsMap(kvs []attribute.KeyValue) map[string]any {
	out := make(map[string]any, len(kvs))
	for _, kv := range kvs {
		out[string(kv.Key)] = kv.Value.AsInterface()
	}
	return out
}

// silence unused import for time when tests are refactored.
var _ = time.Second
