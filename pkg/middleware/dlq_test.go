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

	hirnok "github.com/rlsvr/hirnok/pkg/nats"
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

func mustConnect(t *testing.T, url string) *hirnok.Conn {
	t.Helper()
	c, err := hirnok.Connect(hirnok.Config{URL: url})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

// captureDLQ subscribes to subject via the raw nats.go connection and
// returns a channel that yields each received *nats.Msg.
func captureDLQ(t *testing.T, c *hirnok.Conn, subject string) <-chan *natsio.Msg {
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

const testOrigSubject = "orig.x"

func TestDLQPublishesOnError(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL())
	dlq := captureDLQ(t, c, "dlq.go")

	h := DLQ(func(_ context.Context, _ *natsio.Msg) error {
		return errors.New("boom")
	}, c, "dlq.go", nil)

	m := &natsio.Msg{Subject: testOrigSubject, Data: []byte("payload"), Header: natsio.Header{}}
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

	m := &natsio.Msg{Subject: testOrigSubject, Data: []byte("p"), Header: natsio.Header{}}
	_ = h(context.Background(), m)

	select {
	case dm := <-dlq:
		if v := dm.Header.Get(HeaderDLQReason); v != "bad" {
			t.Errorf("%s = %q, want bad", HeaderDLQReason, v)
		}
		if v := dm.Header.Get(HeaderDLQSubject); v != testOrigSubject {
			t.Errorf("%s = %q, want %s", HeaderDLQSubject, v, testOrigSubject)
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

	h := DLQ(func(_ context.Context, _ *natsio.Msg) error { return hirnok.ErrSkip },
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

func TestDLQDefaultSkipsNakDelay(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL())
	dlq := captureDLQ(t, c, "dlq.nakdelay")

	h := DLQ(func(_ context.Context, _ *natsio.Msg) error {
		return hirnok.NakWithDelay(errors.New("db busy"), 5*time.Second)
	}, c, "dlq.nakdelay", nil)

	m := &natsio.Msg{Subject: "s", Data: []byte("d"), Header: natsio.Header{}}
	err := h(context.Background(), m)
	var nd *hirnok.NakDelayError
	if !errors.As(err, &nd) {
		t.Fatalf("err = %v, want NakDelayError", err)
	}
	select {
	case dm := <-dlq:
		t.Fatalf("NakDelayError should not DLQ; got: %v", dm)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestDLQErrTerminateIsDLQd(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL())
	dlq := captureDLQ(t, c, "dlq.term")

	h := DLQ(func(_ context.Context, _ *natsio.Msg) error {
		return fmt.Errorf("bad: %w", hirnok.ErrTerminate)
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
		fmt.Errorf("bad payload: %w", hirnok.ErrTerminate),
		"dlq.term",
		errors.New("publish connection closed"),
	)
	if errors.Is(err, hirnok.ErrTerminate) {
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
		return fmt.Errorf("unprocessable: %w", hirnok.ErrTerminate)
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
		return fmt.Errorf("bad payload: %w", hirnok.ErrTerminate)
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

func TestDLQCustomHeaders(t *testing.T) {
	const signupSubject = "events.user.signup"
	s := runServer(t)
	c := mustConnect(t, s.ClientURL())
	dlq := captureDLQ(t, c, "dlq.custom")

	h := DLQ(func(_ context.Context, _ *natsio.Msg) error {
		return errors.New("boom")
	}, c, "dlq.custom", nil,
		WithDLQHeaders(map[string]string{
			"topic_poisoned":   signupSubject,
			"handler_poisoned": "user-signup-handler",
		}),
	)

	m := &natsio.Msg{Subject: signupSubject, Data: []byte("payload"), Header: natsio.Header{}}
	if err := h(context.Background(), m); err != nil {
		t.Fatalf("wrapper err = %v, want nil", err)
	}

	select {
	case dm := <-dlq:
		if dm.Header.Get("topic_poisoned") != signupSubject {
			t.Errorf("topic_poisoned = %q", dm.Header.Get("topic_poisoned"))
		}
		if dm.Header.Get("handler_poisoned") != "user-signup-handler" {
			t.Errorf("handler_poisoned = %q", dm.Header.Get("handler_poisoned"))
		}
		if dm.Header.Get(HeaderDLQReason) != "boom" {
			t.Errorf("HeaderDLQReason = %q", dm.Header.Get(HeaderDLQReason))
		}
	case <-time.After(time.Second):
		t.Fatal("no DLQ message")
	}
}

func TestDLQCustomHeaderFunc(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL())
	dlq := captureDLQ(t, c, "dlq.func")

	h := DLQ(func(_ context.Context, _ *natsio.Msg) error {
		return errors.New("custom test fault")
	}, c, "dlq.func", nil,
		WithDLQHeaderFunc(func(h natsio.Header, _ error, origSubject string) {
			h.Set("custom_dynamic", "dyn_"+origSubject)
		}),
	)

	m := &natsio.Msg{Subject: "orders.placed", Data: []byte("d"), Header: natsio.Header{}}
	if err := h(context.Background(), m); err != nil {
		t.Fatalf("wrapper err = %v, want nil", err)
	}

	select {
	case dm := <-dlq:
		if dm.Header.Get("custom_dynamic") != "dyn_orders.placed" {
			t.Errorf("custom_dynamic = %q", dm.Header.Get("custom_dynamic"))
		}
	case <-time.After(time.Second):
		t.Fatal("no DLQ message")
	}
}

func TestDLQ_ReasonTruncationAndSanitation(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL())
	dlq := captureDLQ(t, c, "dlq.sanitize")

	// Multiline error (e.g. panic stack trace)
	multilineErr := errors.New("first line of issue\nsecond line stack trace\nthird line")
	h1 := DLQ(func(_ context.Context, _ *natsio.Msg) error {
		return multilineErr
	}, c, "dlq.sanitize", nil)

	m1 := &natsio.Msg{Subject: "test.multiline", Data: []byte("d1")}
	if err := h1(context.Background(), m1); err != nil {
		t.Fatalf("h1 err = %v, want nil", err)
	}

	select {
	case dm := <-dlq:
		reason := dm.Header.Get(HeaderDLQReason)
		if strings.Contains(reason, "\n") || strings.Contains(reason, "\r") {
			t.Errorf("expected newlines to be stripped from reason, got: %q", reason)
		}
		if reason != "first line of issue" {
			t.Errorf("reason = %q, want %q", reason, "first line of issue")
		}
	case <-time.After(time.Second):
		t.Fatal("no DLQ message for multiline")
	}

	// Long error exceeding 1024 bytes
	longErr := errors.New(strings.Repeat("x", 2000))
	h2 := DLQ(func(_ context.Context, _ *natsio.Msg) error {
		return longErr
	}, c, "dlq.sanitize", nil)

	m2 := &natsio.Msg{Subject: "test.long", Data: []byte("d2")}
	if err := h2(context.Background(), m2); err != nil {
		t.Fatalf("h2 err = %v, want nil", err)
	}

	select {
	case dm := <-dlq:
		reason := dm.Header.Get(HeaderDLQReason)
		if len(reason) > maxDLQReasonLen+len("...(truncated)") {
			t.Errorf("reason length = %d, expected <= %d", len(reason), maxDLQReasonLen+len("...(truncated)"))
		}
		if !strings.HasSuffix(reason, "...(truncated)") {
			t.Errorf("expected reason to end with '...(truncated)', got: %q", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("no DLQ message for long error")
	}
}

func TestDLQ_NilHandlerPanics(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL())

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil handler in DLQ")
		}
	}()

	_ = DLQ(nil, c, "dlq.nil", nil)
}

func TestDLQ_ContextCanceledDoesNotPublish(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL())
	dlq := captureDLQ(t, c, "dlq.canceled")

	h := DLQ(func(_ context.Context, _ *natsio.Msg) error {
		return errors.New("some fault")
	}, c, "dlq.canceled", nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel context

	m := &natsio.Msg{Subject: "test.canceled", Data: []byte("d")}
	err := h(ctx, m)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled error, got: %v", err)
	}

	select {
	case <-dlq:
		t.Fatal("unexpected message published to DLQ when context was canceled")
	case <-time.After(100 * time.Millisecond):
		// Expected: nothing published
	}
}

func TestDLQJet_StripsMsgIDAndPreventsSelfLoop(t *testing.T) {
	s := runJSServer(t)
	c := mustConnect(t, s.ClientURL())
	js, err := c.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	_, err = js.Raw().CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:     "ORDERS_DLQ_TEST",
		Subjects: []string{"orders.>"},
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}

	var calls atomic.Int32
	h := DLQJet(func(_ context.Context, _ jetstream.Msg) error {
		calls.Add(1)
		return fmt.Errorf("poison: %w", hirnok.ErrTerminate)
	}, js, "orders.dlq", nil)

	cfg := hirnok.DefaultConsumerConfig("orders-worker")
	cfg.FilterSubject = "orders.>"
	cons, err := js.NewConsumer(context.Background(), "ORDERS_DLQ_TEST", cfg, h)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer cons.Stop()

	msg := natsio.NewMsg("orders.created")
	msg.Data = []byte(`{"id":1}`)
	msg.Header.Set(jetstream.MsgIDHeader, "dedupe-order-1")
	msg.Header.Set(jetstream.ExpectedStreamHeader, "ORDERS_DLQ_TEST")
	if _, err := js.PublishMsg(context.Background(), msg); err != nil {
		t.Fatalf("publish msg: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	if got := calls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1 (DLQ message must not trigger self-consumption loop)", got)
	}

	stream, err := js.Raw().Stream(context.Background(), "ORDERS_DLQ_TEST")
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	rawMsg, err := stream.GetLastMsgForSubject(context.Background(), "orders.dlq")
	if err != nil {
		t.Fatalf("expected DLQ message on orders.dlq (not deduplicated by Nats-Msg-Id): %v", err)
	}
	if got := rawMsg.Header.Get(HeaderDLQMessageID); got != "dedupe-order-1" {
		t.Fatalf("%s = %q, want dedupe-order-1", HeaderDLQMessageID, got)
	}
	if got := rawMsg.Header.Get(jetstream.MsgIDHeader); got != "" {
		t.Fatalf("expected %s to be stripped on DLQ message, got %q", jetstream.MsgIDHeader, got)
	}
}
