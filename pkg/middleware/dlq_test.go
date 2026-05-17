package middleware

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	qpnats "github.com/rlsvr/hirnok/pkg/nats"
)

// ---------------- helpers ----------------

func runServer(t *testing.T) *natsserver.Server {
	t.Helper()
	s := natstest.RunRandClientPortServer()
	t.Cleanup(s.Shutdown)
	return s
}

func runJSServer(t *testing.T) *natsserver.Server {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	s := natstest.RunServer(&opts)
	t.Cleanup(s.Shutdown)
	return s
}

func mustConnect(t *testing.T, url string) *qpnats.Conn {
	t.Helper()
	c, err := qpnats.Connect(qpnats.Config{URL: url})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

// captureDLQ subscribes to subject via the raw nats.go connection and
// returns a channel that yields each received *nats.Msg.
func captureDLQ(t *testing.T, c *qpnats.Conn, subject string) <-chan *natsio.Msg {
	t.Helper()
	ch := make(chan *natsio.Msg, 4)
	sub, err := c.Raw().Subscribe(subject, func(m *natsio.Msg) {
		ch <- m
	})
	if err != nil {
		t.Fatalf("dlq subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	return ch
}

// ---------------- DLQ (core NATS) ----------------

func TestDLQPublishesOnError(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL())
	dlq := captureDLQ(t, c, "dlq.go")

	h := DLQ(func(_ context.Context, _ *natsio.Msg) error {
		return errors.New("boom")
	}, c, "dlq.go", nil)

	m := &natsio.Msg{Subject: "orig.x", Data: []byte("payload"), Header: natsio.Header{}}
	if err := h(context.Background(), m); err != nil {
		t.Fatalf("wrapper err = %v, want nil after DLQ", err)
	}

	select {
	case dm := <-dlq:
		if string(dm.Data) != "payload" {
			t.Fatalf("data = %q, want payload", dm.Data)
		}
	case <-time.After(time.Second):
		t.Fatal("no DLQ message received")
	}
}

func TestDLQAddsHeaders(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL())
	dlq := captureDLQ(t, c, "dlq.hdr")

	h := DLQ(func(_ context.Context, _ *natsio.Msg) error {
		return errors.New("bad")
	}, c, "dlq.hdr", nil)

	m := &natsio.Msg{Subject: "orig.x", Data: []byte("p"), Header: natsio.Header{}}
	_ = h(context.Background(), m)

	select {
	case dm := <-dlq:
		if v := dm.Header.Get(HeaderDLQReason); v != "bad" {
			t.Errorf("%s = %q, want bad", HeaderDLQReason, v)
		}
		if v := dm.Header.Get(HeaderDLQSubject); v != "orig.x" {
			t.Errorf("%s = %q, want orig.x", HeaderDLQSubject, v)
		}
		if v := dm.Header.Get(HeaderDLQAt); v == "" {
			t.Errorf("%s is empty", HeaderDLQAt)
		} else if _, err := time.Parse(time.RFC3339, v); err != nil {
			t.Errorf("%s = %q not RFC3339: %v", HeaderDLQAt, v, err)
		}
	case <-time.After(time.Second):
		t.Fatal("no DLQ message")
	}
}

func TestDLQPreservesOriginalHeaders(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL())
	dlq := captureDLQ(t, c, "dlq.preserve")

	h := DLQ(func(_ context.Context, _ *natsio.Msg) error { return errors.New("x") },
		c, "dlq.preserve", nil)

	hdr := natsio.Header{}
	hdr.Set("traceparent", "00-trace-span-01")
	hdr.Set("X-Custom", "keep-me")
	m := &natsio.Msg{Subject: "s", Data: []byte("d"), Header: hdr}
	_ = h(context.Background(), m)

	select {
	case dm := <-dlq:
		if v := dm.Header.Get("traceparent"); v != "00-trace-span-01" {
			t.Errorf("traceparent lost: %q", v)
		}
		if v := dm.Header.Get("X-Custom"); v != "keep-me" {
			t.Errorf("X-Custom lost: %q", v)
		}
	case <-time.After(time.Second):
		t.Fatal("no DLQ message")
	}
}

func TestDLQPassThroughOnNil(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL())
	dlq := captureDLQ(t, c, "dlq.ok")

	h := DLQ(func(_ context.Context, _ *natsio.Msg) error { return nil },
		c, "dlq.ok", nil)

	m := &natsio.Msg{Subject: "s", Data: []byte("d"), Header: natsio.Header{}}
	if err := h(context.Background(), m); err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	select {
	case dm := <-dlq:
		t.Fatalf("unexpected DLQ message: %v", dm)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestDLQErrSkipShortCircuits(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL())
	dlq := captureDLQ(t, c, "dlq.skip")

	h := DLQ(func(_ context.Context, _ *natsio.Msg) error { return qpnats.ErrSkip },
		c, "dlq.skip", nil)

	m := &natsio.Msg{Subject: "s", Data: []byte("d"), Header: natsio.Header{}}
	if err := h(context.Background(), m); err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	select {
	case dm := <-dlq:
		t.Fatalf("ErrSkip should not DLQ; got: %v", dm)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestDLQShouldDLQFalse(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL())
	dlq := captureDLQ(t, c, "dlq.false")

	sentinel := errors.New("transient")
	h := DLQ(func(_ context.Context, _ *natsio.Msg) error { return sentinel },
		c, "dlq.false", func(_ error) bool { return false })

	m := &natsio.Msg{Subject: "s", Data: []byte("d"), Header: natsio.Header{}}
	if err := h(context.Background(), m); !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel (should bubble up)", err)
	}
	select {
	case dm := <-dlq:
		t.Fatalf("shouldDLQ=false should not publish; got: %v", dm)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestDLQDefaultSkipsCtxCanceled(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL())
	dlq := captureDLQ(t, c, "dlq.cancel")

	h := DLQ(func(_ context.Context, _ *natsio.Msg) error { return context.Canceled },
		c, "dlq.cancel", nil)

	m := &natsio.Msg{Subject: "s", Data: []byte("d"), Header: natsio.Header{}}
	if err := h(context.Background(), m); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	select {
	case dm := <-dlq:
		t.Fatalf("ctx.Canceled should not DLQ; got: %v", dm)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestDLQDefaultSkipsCtxDeadline(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL())
	dlq := captureDLQ(t, c, "dlq.deadline")

	h := DLQ(func(_ context.Context, _ *natsio.Msg) error { return context.DeadlineExceeded },
		c, "dlq.deadline", nil)

	m := &natsio.Msg{Subject: "s", Data: []byte("d"), Header: natsio.Header{}}
	if err := h(context.Background(), m); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	select {
	case <-dlq:
		t.Fatal("ctx.DeadlineExceeded should not DLQ")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestDLQErrTerminateIsDLQd(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL())
	dlq := captureDLQ(t, c, "dlq.term")

	h := DLQ(func(_ context.Context, _ *natsio.Msg) error {
		return fmt.Errorf("bad: %w", qpnats.ErrTerminate)
	}, c, "dlq.term", nil)

	m := &natsio.Msg{Subject: "s", Data: []byte("d"), Header: natsio.Header{}}
	if err := h(context.Background(), m); err != nil {
		t.Fatalf("wrapper err = %v, want nil", err)
	}
	select {
	case dm := <-dlq:
		if !strings.Contains(dm.Header.Get(HeaderDLQReason), "terminate") {
			t.Errorf("reason = %q, expected to mention terminate", dm.Header.Get(HeaderDLQReason))
		}
	case <-time.After(time.Second):
		t.Fatal("ErrTerminate should DLQ")
	}
}

func TestDLQPublishFailureReturnsJoined(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL())

	// Close the conn so PublishMsg fails.
	c.Close()
	time.Sleep(20 * time.Millisecond) // let Drain settle

	handlerErr := errors.New("original")
	h := DLQ(func(_ context.Context, _ *natsio.Msg) error { return handlerErr },
		c, "dlq.broken", nil)

	m := &natsio.Msg{Subject: "s", Data: []byte("d"), Header: natsio.Header{}}
	err := h(context.Background(), m)
	if err == nil {
		t.Fatal("expected error when DLQ publish fails")
	}
	if !errors.Is(err, handlerErr) {
		t.Errorf("returned err does not wrap handler err: %v", err)
	}
}

func TestDLQPublishFailureStripsErrTerminate(t *testing.T) {
	err := dlqPublishError(
		fmt.Errorf("bad payload: %w", qpnats.ErrTerminate),
		"dlq.term",
		errors.New("publish failed"),
	)
	if errors.Is(err, qpnats.ErrTerminate) {
		t.Fatalf("publish failure error should not preserve ErrTerminate: %v", err)
	}
	if !strings.Contains(err.Error(), "terminate") {
		t.Fatalf("error should keep handler context text, got %v", err)
	}
}

func TestDLQPanicsOnNilConn(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for nil conn")
		}
	}()
	_ = DLQ(func(_ context.Context, _ *natsio.Msg) error { return nil }, nil, "x", nil)
}

func TestDLQPanicsOnEmptySubject(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL())
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for empty subject")
		}
	}()
	_ = DLQ(func(_ context.Context, _ *natsio.Msg) error { return nil }, c, "", nil)
}

// ---------------- DLQJet ----------------

func TestDLQJetPublishesOnError(t *testing.T) {
	s := runJSServer(t)
	c := mustConnect(t, s.ClientURL())
	j, err := c.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	if _, err := j.Raw().CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:     "DLQ_S",
		Subjects: []string{"dlq.js.>"},
	}); err != nil {
		t.Fatalf("stream: %v", err)
	}
	dlq := captureDLQ(t, c, "dlq.js.x")

	h := DLQJet(func(_ context.Context, _ jetstream.Msg) error {
		return fmt.Errorf("unprocessable: %w", qpnats.ErrTerminate)
	}, j, "dlq.js.x", nil)

	// Build a stub jetstream.Msg with everything DLQ needs.
	hdr := natsio.Header{}
	hdr.Set("traceparent", "00-trace-01")
	jm := &dlqJetStub{
		subject: "orders.new",
		data:    []byte("ord-1"),
		headers: hdr,
	}

	if err := h(context.Background(), jm); err != nil {
		t.Fatalf("wrapper err = %v, want nil", err)
	}

	select {
	case dm := <-dlq:
		if string(dm.Data) != "ord-1" {
			t.Errorf("data = %q", dm.Data)
		}
		if dm.Header.Get(HeaderDLQSubject) != "orders.new" {
			t.Errorf("orig subject lost: %q", dm.Header.Get(HeaderDLQSubject))
		}
		if dm.Header.Get("traceparent") != "00-trace-01" {
			t.Errorf("traceparent lost")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no DLQ message")
	}
}

// dlqJetStub implements just enough of jetstream.Msg for DLQJet to read
// Subject/Data/Headers. Other methods inherit the nil interface and
// will panic if called — we never call them.
type dlqJetStub struct {
	jetstream.Msg
	subject string
	data    []byte
	headers natsio.Header
}

func (s *dlqJetStub) Subject() string        { return s.subject }
func (s *dlqJetStub) Data() []byte           { return s.data }
func (s *dlqJetStub) Headers() natsio.Header { return s.headers }

// ---------------- Composition ----------------

func TestRetryThenDLQ(t *testing.T) {
	// DLQ outside Retry: handler always fails → Retry exhausts → DLQ
	// catches the final failure and republishes once.
	s := runServer(t)
	c := mustConnect(t, s.ClientURL())
	dlq := captureDLQ(t, c, "dlq.retried")

	var calls atomic.Int32
	inner := func(_ context.Context, _ *natsio.Msg) error {
		calls.Add(1)
		return errors.New("transient")
	}
	h := DLQ(Retry(inner, 3, time.Millisecond, 0, nil), c, "dlq.retried", nil)

	m := &natsio.Msg{Subject: "s", Data: []byte("d"), Header: natsio.Header{}}
	if err := h(context.Background(), m); err != nil {
		t.Fatalf("wrapper err = %v, want nil", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("calls = %d, want 3", got)
	}
	select {
	case <-dlq:
	case <-time.After(time.Second):
		t.Fatal("no DLQ message after retry exhaustion")
	}
}

func TestDLQInsideRetry(t *testing.T) {
	// DLQ inside Retry: handler always fails → DLQ swallows each
	// attempt → Retry sees nil → done. Handler called once, one
	// DLQ message published.
	s := runServer(t)
	c := mustConnect(t, s.ClientURL())
	dlq := captureDLQ(t, c, "dlq.swallow")

	var calls atomic.Int32
	inner := func(_ context.Context, _ *natsio.Msg) error {
		calls.Add(1)
		return errors.New("transient")
	}
	h := Retry(DLQ(inner, c, "dlq.swallow", nil), 5, time.Millisecond, 0, nil)

	m := &natsio.Msg{Subject: "s", Data: []byte("d"), Header: natsio.Header{}}
	if err := h(context.Background(), m); err != nil {
		t.Fatalf("wrapper err = %v, want nil", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1 (DLQ swallowed; no retry)", got)
	}
	select {
	case <-dlq:
	case <-time.After(time.Second):
		t.Fatal("no DLQ message")
	}
}

func TestErrTerminateBypassesRetry(t *testing.T) {
	// Canonical pattern: Retry(DLQ(h)). Handler returns ErrTerminate.
	// defaultShouldRetry skips terminate; defaultShouldDLQ accepts it.
	// DLQ publishes; Retry sees nil; handler called exactly once.
	s := runServer(t)
	c := mustConnect(t, s.ClientURL())
	dlq := captureDLQ(t, c, "dlq.terminate")

	var calls atomic.Int32
	inner := func(_ context.Context, _ *natsio.Msg) error {
		calls.Add(1)
		return fmt.Errorf("bad payload: %w", qpnats.ErrTerminate)
	}
	h := Retry(DLQ(inner, c, "dlq.terminate", nil), 5, time.Millisecond, 0, nil)

	m := &natsio.Msg{Subject: "s", Data: []byte("d"), Header: natsio.Header{}}
	if err := h(context.Background(), m); err != nil {
		t.Fatalf("wrapper err = %v, want nil", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1 (terminate bypasses retry)", got)
	}
	select {
	case <-dlq:
	case <-time.After(time.Second):
		t.Fatal("no DLQ message")
	}
}
