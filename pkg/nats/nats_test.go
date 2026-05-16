package nats

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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

func mustConnect(t *testing.T, url string, cfg Config) *Conn {
	t.Helper()
	cfg.URL = url
	c, err := Connect(cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

func ensureStream(t *testing.T, j *JetStream, name string, subjects ...string) {
	t.Helper()
	_, err := j.Raw().CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:     name,
		Subjects: subjects,
	})
	if err != nil {
		t.Fatalf("ensure stream %s: %v", name, err)
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", timeout)
}

// ---------------- core NATS tests ----------------

func TestConnectCloseSmoke(t *testing.T) {
	s := runServer(t)
	c, err := Connect(Config{URL: s.ClientURL(), Name: "test"})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	c.Close()
}

func TestConnectRequiresURL(t *testing.T) {
	_, err := Connect(Config{})
	if err == nil {
		t.Fatal("expected error for empty URL")
	}
}

func TestSubscribePublishRoundTrip(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL(), Config{})

	got := make(chan *Message, 1)
	sub, err := c.Subscribe(context.Background(), "foo", func(_ context.Context, m *Message) error {
		got <- m
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe() //nolint:errcheck

	if err := c.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case m := <-got:
		if string(m.Data) != "hello" {
			t.Fatalf("Data=%q want hello", m.Data)
		}
		if m.Subject != "foo" {
			t.Fatalf("Subject=%q want foo", m.Subject)
		}
	case <-time.After(time.Second):
		t.Fatal("no message received")
	}
}

func TestPublishMsgPreservesHeaders(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL(), Config{})

	got := make(chan *Message, 1)
	sub, err := c.Subscribe(context.Background(), "hdr", func(_ context.Context, m *Message) error {
		got <- m
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe() //nolint:errcheck

	msg := &Message{Msg: &natsio.Msg{Subject: "hdr", Data: []byte("body"), Header: natsio.Header{}}}
	msg.Header.Set("X-Foo", "bar")
	if err := c.PublishMsg(msg); err != nil {
		t.Fatalf("PublishMsg: %v", err)
	}

	select {
	case m := <-got:
		if v := m.Header.Get("X-Foo"); v != "bar" {
			t.Fatalf("X-Foo=%q want bar", v)
		}
	case <-time.After(time.Second):
		t.Fatal("no message received")
	}
}

func TestPublishMsgNilRejected(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL(), Config{})
	if err := c.PublishMsg(nil); err == nil {
		t.Fatal("expected error for nil Message")
	}
	if err := c.PublishMsg(&Message{}); err == nil {
		t.Fatal("expected error for Message with nil Msg")
	}
}

func TestRequestReply(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL(), Config{})

	sub, err := c.Subscribe(context.Background(), "echo", func(_ context.Context, m *Message) error {
		return m.Respond(append([]byte("re:"), m.Data...))
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe() //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	reply, err := c.Request(ctx, "echo", []byte("hi"))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if string(reply.Data) != "re:hi" {
		t.Fatalf("reply=%q want re:hi", reply.Data)
	}
}

func TestMultiWorkerFanOut(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL(), Config{QueueSize: 64, Workers: 4})

	const total = 100
	var received atomic.Int32
	var peak atomic.Int32
	var inflight atomic.Int32

	sub, err := c.Subscribe(context.Background(), "fan", func(_ context.Context, _ *Message) error {
		cur := inflight.Add(1)
		for {
			old := peak.Load()
			if cur <= old || peak.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		inflight.Add(-1)
		received.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe() //nolint:errcheck

	for range total {
		if err := c.Publish("fan", []byte("x")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	waitFor(t, 5*time.Second, func() bool { return received.Load() == total })

	if peak.Load() < 2 {
		t.Fatalf("peak concurrency %d, want >= 2", peak.Load())
	}
}

func TestHandlerErrorDoesNotStopWorkers(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL(), Config{Workers: 1})

	var seen atomic.Int32
	sub, err := c.Subscribe(context.Background(), "err", func(_ context.Context, _ *Message) error {
		n := seen.Add(1)
		if n%2 == 1 {
			return errors.New("boom")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe() //nolint:errcheck

	for range 6 {
		if err := c.Publish("err", []byte("x")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	waitFor(t, 2*time.Second, func() bool { return seen.Load() == 6 })
}

func TestUnsubscribeStopsWorkers(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL(), Config{Workers: 2})

	var seen atomic.Int32
	sub, err := c.Subscribe(context.Background(), "unsub", func(_ context.Context, _ *Message) error {
		seen.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if err := c.Publish("unsub", []byte("x")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	waitFor(t, time.Second, func() bool { return seen.Load() == 1 })

	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}
	select {
	case <-sub.Stopped():
	case <-time.After(time.Second):
		t.Fatal("workers did not stop after Unsubscribe")
	}

	if err := c.Publish("unsub", []byte("x")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if got := seen.Load(); got != 1 {
		t.Fatalf("handler ran after unsubscribe: seen=%d", got)
	}
}

func TestCloseStopsAllSubs(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL(), Config{Workers: 1})

	var seen atomic.Int32
	for _, subj := range []string{"a", "b"} {
		_, err := c.Subscribe(context.Background(), subj, func(_ context.Context, _ *Message) error {
			seen.Add(1)
			return nil
		})
		if err != nil {
			t.Fatalf("subscribe %s: %v", subj, err)
		}
	}
	if err := c.Publish("a", []byte("x")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	waitFor(t, time.Second, func() bool { return seen.Load() >= 1 })
	c.Close()
	before := seen.Load()
	time.Sleep(50 * time.Millisecond)
	if seen.Load() != before {
		t.Fatalf("handler ran after Close")
	}
}

func TestQueueSubscribeRequiresGroup(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL(), Config{})
	_, err := c.QueueSubscribe(context.Background(), "g", "", func(_ context.Context, _ *Message) error { return nil })
	if err == nil {
		t.Fatal("expected error for empty group")
	}
}

func TestQueueSubscribeLoadBalances(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL(), Config{Workers: 1})

	var a, b atomic.Int32
	subA, err := c.QueueSubscribe(context.Background(), "lb", "grp", func(_ context.Context, _ *Message) error {
		a.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("subA: %v", err)
	}
	defer subA.Unsubscribe() //nolint:errcheck

	subB, err := c.QueueSubscribe(context.Background(), "lb", "grp", func(_ context.Context, _ *Message) error {
		b.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("subB: %v", err)
	}
	defer subB.Unsubscribe() //nolint:errcheck

	const total = 50
	for range total {
		if err := c.Publish("lb", []byte("x")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	waitFor(t, 2*time.Second, func() bool { return a.Load()+b.Load() == total })

	if a.Load() == 0 || b.Load() == 0 {
		t.Fatalf("expected both subs to receive, got a=%d b=%d", a.Load(), b.Load())
	}
}

func TestSubscribeRespectsContext(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL(), Config{Workers: 1})

	ctx, cancel := context.WithCancel(context.Background())
	var seen atomic.Int32
	sub, err := c.Subscribe(ctx, "ctx", func(_ context.Context, _ *Message) error {
		seen.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if err := c.Publish("ctx", []byte("x")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	waitFor(t, time.Second, func() bool { return seen.Load() == 1 })

	cancel()
	select {
	case <-sub.Stopped():
	case <-time.After(time.Second):
		t.Fatal("workers did not stop after ctx cancel")
	}
}

func TestSubWaitUnblocksAfterUnsubscribe(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL(), Config{Workers: 2})

	sub, err := c.Subscribe(context.Background(), "wait", func(_ context.Context, _ *Message) error { return nil })
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}
	if err := sub.Wait(ctx); err != nil {
		t.Fatalf("wait: %v", err)
	}
}

func TestSubWaitRespectsCtx(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL(), Config{Workers: 1})
	sub, err := c.Subscribe(context.Background(), "wait2", func(_ context.Context, _ *Message) error { return nil })
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe() //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := sub.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait err = %v, want DeadlineExceeded", err)
	}
}

func TestQueueSizeBackpressure(t *testing.T) {
	s := runServer(t)
	c := mustConnect(t, s.ClientURL(), Config{QueueSize: 1, Workers: 1})

	release := make(chan struct{})
	var processed atomic.Int32
	var wg sync.WaitGroup
	wg.Add(1)
	first := make(chan struct{}, 1)
	sub, err := c.Subscribe(context.Background(), "bp", func(_ context.Context, _ *Message) error {
		select {
		case first <- struct{}{}:
		default:
		}
		<-release
		processed.Add(1)
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe() //nolint:errcheck

	if err := c.Publish("bp", []byte("x")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case <-first:
	case <-time.After(time.Second):
		t.Fatal("first message did not arrive")
	}

	for range 4 {
		if err := c.Publish("bp", []byte("x")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	time.Sleep(50 * time.Millisecond)
	if processed.Load() != 0 {
		t.Fatalf("processed should be 0 while handler blocked, got %d", processed.Load())
	}

	wg.Add(4)
	close(release)
	wg.Wait()
	if processed.Load() != 5 {
		t.Fatalf("processed=%d want 5", processed.Load())
	}
}
