package nats

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func mustJetStream(t *testing.T, c *Conn) *JetStream {
	t.Helper()
	j, err := c.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	return j
}

func TestJetStreamContext(t *testing.T) {
	s := runJSServer(t)
	c := mustConnect(t, s.ClientURL(), Config{})
	j1 := mustJetStream(t, c)
	j2 := mustJetStream(t, c)
	if j1 == nil || j2 == nil {
		t.Fatal("nil JetStream")
	}
}

func TestNewConsumerAckOnNil(t *testing.T) {
	s := runJSServer(t)
	c := mustConnect(t, s.ClientURL(), Config{Workers: 2})
	j := mustJetStream(t, c)
	ensureStream(t, j, "S1", "s1.>")

	const total = 5
	for i := range total {
		if _, err := j.Publish(context.Background(), "s1.x", fmt.Appendf(nil, "%d", i)); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	var seen atomic.Int32
	cons, err := j.NewConsumer(context.Background(), "S1", jetstream.ConsumerConfig{
		Durable:       "c1",
		FilterSubject: "s1.>",
		AckWait:       2 * time.Second,
	}, func(_ context.Context, _ jetstream.Msg) error {
		seen.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer cons.Stop()

	waitFor(t, 5*time.Second, func() bool { return seen.Load() == total })

	stream, err := j.Raw().Stream(context.Background(), "S1")
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	consInfo, err := stream.Consumer(context.Background(), "c1")
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	info, err := consInfo.Info(context.Background())
	if err != nil {
		t.Fatalf("info: %v", err)
	}
	if info.NumPending != 0 || info.NumAckPending != 0 {
		t.Fatalf("pending=%d ackPending=%d, want 0", info.NumPending, info.NumAckPending)
	}
}

func TestNewConsumerNakOnError(t *testing.T) {
	s := runJSServer(t)
	c := mustConnect(t, s.ClientURL(), Config{Workers: 1})
	j := mustJetStream(t, c)
	ensureStream(t, j, "S2", "s2.>")

	if _, err := j.Publish(context.Background(), "s2.x", []byte("once")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	var seen atomic.Int32
	delivered := make(chan uint64, 4)
	cons, err := j.NewConsumer(context.Background(), "S2", jetstream.ConsumerConfig{
		Durable:       "c2",
		FilterSubject: "s2.>",
		AckWait:       500 * time.Millisecond,
	}, func(_ context.Context, m jetstream.Msg) error {
		md, _ := m.Metadata()
		delivered <- md.NumDelivered
		if seen.Add(1) == 1 {
			return errors.New("nak me")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer cons.Stop()

	waitFor(t, 5*time.Second, func() bool { return seen.Load() >= 2 })

	close(delivered)
	var maxN uint64
	for n := range delivered {
		if n > maxN {
			maxN = n
		}
	}
	if maxN < 2 {
		t.Fatalf("NumDelivered never reached 2 (got max %d) - Nak did not trigger redelivery", maxN)
	}
}

func TestConsumerUpsertIdempotent(t *testing.T) {
	s := runJSServer(t)
	c := mustConnect(t, s.ClientURL(), Config{})
	j := mustJetStream(t, c)
	ensureStream(t, j, "S3", "s3.>")

	cfg := jetstream.ConsumerConfig{
		Durable:       "c3",
		FilterSubject: "s3.>",
		AckWait:       2 * time.Second,
	}
	cons1, err := j.NewConsumer(context.Background(), "S3", cfg, func(_ context.Context, _ jetstream.Msg) error { return nil })
	if err != nil {
		t.Fatalf("first new consumer: %v", err)
	}
	cons1.Stop()

	cfg.AckWait = 5 * time.Second
	cons2, err := j.NewConsumer(context.Background(), "S3", cfg, func(_ context.Context, _ jetstream.Msg) error { return nil })
	if err != nil {
		t.Fatalf("second new consumer (upsert): %v", err)
	}
	cons2.Stop()
}

func TestConsumerStopHaltsDelivery(t *testing.T) {
	s := runJSServer(t)
	c := mustConnect(t, s.ClientURL(), Config{Workers: 1})
	j := mustJetStream(t, c)
	ensureStream(t, j, "S4", "s4.>")

	if _, err := j.Publish(context.Background(), "s4.x", []byte("first")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	var seen atomic.Int32
	cons, err := j.NewConsumer(context.Background(), "S4", jetstream.ConsumerConfig{
		Durable:       "c4",
		FilterSubject: "s4.>",
		AckWait:       2 * time.Second,
	}, func(_ context.Context, _ jetstream.Msg) error {
		seen.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return seen.Load() == 1 })
	cons.Stop()

	if _, err := j.Publish(context.Background(), "s4.x", []byte("after")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if seen.Load() != 1 {
		t.Fatalf("handler ran after Stop: seen=%d", seen.Load())
	}
}

func TestPublishReturnsPubAck(t *testing.T) {
	s := runJSServer(t)
	c := mustConnect(t, s.ClientURL(), Config{})
	j := mustJetStream(t, c)
	ensureStream(t, j, "S5", "s5.>")

	ack, err := j.Publish(context.Background(), "s5.x", []byte("p"))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if ack.Sequence == 0 {
		t.Fatalf("Sequence=0, want > 0")
	}
}

func TestPublishMsgDedupe(t *testing.T) {
	s := runJSServer(t)
	c := mustConnect(t, s.ClientURL(), Config{})
	j := mustJetStream(t, c)
	ensureStream(t, j, "S6", "s6.>")

	build := func() *natsio.Msg {
		m := &natsio.Msg{Subject: "s6.x", Data: []byte("d"), Header: natsio.Header{}}
		m.Header.Set(jetstream.MsgIDHeader, "dedupe-key-1")
		return m
	}

	ack1, err := j.PublishMsg(context.Background(), build())
	if err != nil {
		t.Fatalf("publish1: %v", err)
	}
	ack2, err := j.PublishMsg(context.Background(), build())
	if err != nil {
		t.Fatalf("publish2: %v", err)
	}
	if !ack2.Duplicate {
		t.Fatalf("second publish should be Duplicate; got ack1=%+v ack2=%+v", ack1, ack2)
	}
}

func TestPublishMsgNilRejectedJS(t *testing.T) {
	s := runJSServer(t)
	c := mustConnect(t, s.ClientURL(), Config{})
	j := mustJetStream(t, c)
	if _, err := j.PublishMsg(context.Background(), nil); err == nil {
		t.Fatal("expected error for nil msg")
	}
}

func TestPublishAsync(t *testing.T) {
	s := runJSServer(t)
	c := mustConnect(t, s.ClientURL(), Config{})
	j := mustJetStream(t, c)
	ensureStream(t, j, "S7", "s7.>")

	const total = 50
	futures := make([]jetstream.PubAckFuture, 0, total)
	for i := range total {
		f, err := j.PublishAsync("s7.x", fmt.Appendf(nil, "%d", i))
		if err != nil {
			t.Fatalf("publishAsync: %v", err)
		}
		futures = append(futures, f)
	}
	for i, f := range futures {
		select {
		case <-f.Ok():
		case err := <-f.Err():
			t.Fatalf("future %d err: %v", i, err)
		case <-time.After(5 * time.Second):
			t.Fatalf("future %d timed out", i)
		}
	}
}

func TestNewConsumerRespectsContext(t *testing.T) {
	s := runJSServer(t)
	c := mustConnect(t, s.ClientURL(), Config{Workers: 1})
	j := mustJetStream(t, c)
	ensureStream(t, j, "S8", "s8.>")

	ctx, cancel := context.WithCancel(context.Background())
	cons, err := j.NewConsumer(ctx, "S8", jetstream.ConsumerConfig{
		Durable:       "c8",
		FilterSubject: "s8.>",
		AckWait:       2 * time.Second,
	}, func(_ context.Context, _ jetstream.Msg) error { return nil })
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	cancel()
	select {
	case <-cons.Stopped():
	case <-time.After(2 * time.Second):
		t.Fatal("worker pool did not stop after ctx cancel")
	}
}

func TestNewConsumerFilterSubject(t *testing.T) {
	s := runJSServer(t)
	c := mustConnect(t, s.ClientURL(), Config{Workers: 1})
	j := mustJetStream(t, c)
	ensureStream(t, j, "S9", "s9.>")

	if _, err := j.Publish(context.Background(), "s9.bar", []byte("yes")); err != nil {
		t.Fatalf("publish bar: %v", err)
	}
	if _, err := j.Publish(context.Background(), "s9.baz", []byte("no")); err != nil {
		t.Fatalf("publish baz: %v", err)
	}

	got := make(chan string, 4)
	cons, err := j.NewConsumer(context.Background(), "S9", jetstream.ConsumerConfig{
		Durable:       "c9",
		FilterSubject: "s9.bar",
		AckWait:       2 * time.Second,
	}, func(_ context.Context, m jetstream.Msg) error {
		got <- m.Subject()
		return nil
	})
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer cons.Stop()

	select {
	case subj := <-got:
		if subj != "s9.bar" {
			t.Fatalf("got %q, want s9.bar", subj)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no message")
	}

	select {
	case subj := <-got:
		t.Fatalf("unexpected second message: %q", subj)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestHandlerCtxCancelsAtAckWait(t *testing.T) {
	s := runJSServer(t)
	c := mustConnect(t, s.ClientURL(), Config{
		Workers:   1,
		AckBuffer: 50 * time.Millisecond,
	})
	j := mustJetStream(t, c)
	ensureStream(t, j, "S10", "s10.>")

	if _, err := j.Publish(context.Background(), "s10.x", []byte("z")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	ctxErrs := make(chan error, 4)
	deliveries := make(chan uint64, 4)
	cons, err := j.NewConsumer(context.Background(), "S10", jetstream.ConsumerConfig{
		Durable:       "c10",
		FilterSubject: "s10.>",
		AckWait:       300 * time.Millisecond,
	}, func(ctx context.Context, m jetstream.Msg) error {
		md, _ := m.Metadata()
		deliveries <- md.NumDelivered
		<-ctx.Done()
		ctxErrs <- ctx.Err()
		return nil
	})
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer cons.Stop()

	select {
	case err := <-ctxErrs:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("ctx.Err()=%v want DeadlineExceeded", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler ctx never canceled")
	}

	waitFor(t, 5*time.Second, func() bool {
		select {
		case n := <-deliveries:
			return n >= 2
		default:
			return false
		}
	})
}

func TestHandlerCtxCancelsOnStop(t *testing.T) {
	s := runJSServer(t)
	c := mustConnect(t, s.ClientURL(), Config{Workers: 1, AckBuffer: 100 * time.Millisecond})
	j := mustJetStream(t, c)
	ensureStream(t, j, "S11", "s11.>")

	if _, err := j.Publish(context.Background(), "s11.x", []byte("z")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	started := make(chan struct{})
	ctxErr := make(chan error, 1)
	cons, err := j.NewConsumer(context.Background(), "S11", jetstream.ConsumerConfig{
		Durable:       "c11",
		FilterSubject: "s11.>",
		AckWait:       30 * time.Second,
	}, func(ctx context.Context, _ jetstream.Msg) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		ctxErr <- ctx.Err()
		return nil
	})
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}

	stopDone := make(chan struct{})
	go func() { cons.Stop(); close(stopDone) }()
	select {
	case <-stopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return")
	}
	select {
	case err := <-ctxErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ctx.Err()=%v want Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("handler did not observe ctx cancel after Stop")
	}
}

func TestConsumerWaitAfterStop(t *testing.T) {
	s := runJSServer(t)
	c := mustConnect(t, s.ClientURL(), Config{Workers: 2})
	j := mustJetStream(t, c)
	ensureStream(t, j, "S12", "s12.>")

	cons, err := j.NewConsumer(context.Background(), "S12", jetstream.ConsumerConfig{
		Durable:       "c12",
		FilterSubject: "s12.>",
		AckWait:       2 * time.Second,
	}, func(_ context.Context, _ jetstream.Msg) error { return nil })
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	cons.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := cons.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
}

func TestConsumerDrainWaitsForInFlightWithoutCancel(t *testing.T) {
	s := runJSServer(t)
	c := mustConnect(t, s.ClientURL(), Config{Workers: 1})
	j := mustJetStream(t, c)
	ensureStream(t, j, "S_DRAIN", "s_drain.>")

	if _, err := j.Publish(context.Background(), "s_drain.x", []byte("z")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	ctxErr := make(chan error, 1)
	cons, err := j.NewConsumer(context.Background(), "S_DRAIN", jetstream.ConsumerConfig{
		Durable:       "c_drain",
		FilterSubject: "s_drain.>",
		AckWait:       30 * time.Second,
	}, func(ctx context.Context, _ jetstream.Msg) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		ctxErr <- ctx.Err()
		return nil
	})
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- cons.Drain(waitCtx) }()

	select {
	case err := <-done:
		t.Fatalf("Drain returned before in-flight handler completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Drain: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Drain did not return")
	}
	if err := <-ctxErr; err != nil {
		t.Fatalf("handler ctx err = %v, want nil", err)
	}
}

func TestNewConsumerOptionsOverrideConfig(t *testing.T) {
	s := runJSServer(t)
	c := mustConnect(t, s.ClientURL(), Config{QueueSize: 64, Workers: 1})
	j := mustJetStream(t, c)
	ensureStream(t, j, "S_OPTS", "s_opts.>")

	cons, err := j.NewConsumer(context.Background(), "S_OPTS", jetstream.ConsumerConfig{
		Durable:       "c_opts",
		FilterSubject: "s_opts.>",
		AckWait:       2 * time.Second,
	}, func(_ context.Context, _ jetstream.Msg) error { return nil }, WithQueueSize(5), WithWorkers(2))
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer cons.Stop()

	if got := cons.q.Cap(); got != 5 {
		t.Fatalf("queue cap = %d, want 5", got)
	}
}

func TestNewPushConsumerReceivesAndAcks(t *testing.T) {
	s := runJSServer(t)
	c := mustConnect(t, s.ClientURL(), Config{Workers: 2})
	j := mustJetStream(t, c)
	ensureStream(t, j, "S_PUSH", "s_push.>")

	var seen atomic.Int32
	gotData := make(chan string, 4)
	cons, err := j.NewPushConsumer(context.Background(), "S_PUSH", jetstream.ConsumerConfig{
		Durable:        "c_push",
		DeliverSubject: natsio.NewInbox(),
		DeliverGroup:   "c_push_group",
		FilterSubject:  "s_push.>",
		AckWait:        2 * time.Second,
	}, func(_ context.Context, m jetstream.Msg) error {
		gotData <- string(m.Data())
		seen.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("new push consumer: %v", err)
	}
	defer cons.Stop()

	if _, err := j.Publish(context.Background(), "s_push.x", []byte("push")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool { return seen.Load() == 1 })
	if got := <-gotData; got != "push" {
		t.Fatalf("data=%q, want push", got)
	}

	stream, err := j.Raw().Stream(context.Background(), "S_PUSH")
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	consInfo, err := stream.PushConsumer(context.Background(), "c_push")
	if err != nil {
		t.Fatalf("push consumer: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool {
		info, err := consInfo.Info(context.Background())
		if err != nil {
			t.Fatalf("info: %v", err)
		}
		return info.NumPending == 0 && info.NumAckPending == 0
	})
}

func TestNewPushConsumerDefaultsDeliverSubject(t *testing.T) {
	s := runJSServer(t)
	c := mustConnect(t, s.ClientURL(), Config{Workers: 1})
	j := mustJetStream(t, c)
	ensureStream(t, j, "S_PUSH_AUTO", "s_push_auto.>")

	var seen atomic.Int32
	cons, err := j.NewPushConsumer(context.Background(), "S_PUSH_AUTO", jetstream.ConsumerConfig{
		Name:          "c_push_auto",
		FilterSubject: "s_push_auto.>",
		AckWait:       2 * time.Second,
	}, func(_ context.Context, _ jetstream.Msg) error {
		seen.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("new push consumer: %v", err)
	}
	defer cons.Stop()

	stream, err := j.Raw().Stream(context.Background(), "S_PUSH_AUTO")
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	consInfo, err := stream.PushConsumer(context.Background(), "c_push_auto")
	if err != nil {
		t.Fatalf("push consumer: %v", err)
	}
	info, err := consInfo.Info(context.Background())
	if err != nil {
		t.Fatalf("info: %v", err)
	}
	if info.Config.DeliverSubject == "" {
		t.Fatal("DeliverSubject was not defaulted")
	}

	if _, err := j.Publish(context.Background(), "s_push_auto.x", []byte("push")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool { return seen.Load() == 1 })
}

func TestConsumeAlias(t *testing.T) {
	s := runJSServer(t)
	c := mustConnect(t, s.ClientURL(), Config{Workers: 1})
	j := mustJetStream(t, c)
	ensureStream(t, j, "S_ALIAS", "s_alias.>")

	cons, err := j.Consume(context.Background(), "S_ALIAS", jetstream.ConsumerConfig{
		Durable:       "c_alias",
		FilterSubject: "s_alias.>",
		AckWait:       2 * time.Second,
	}, func(_ context.Context, _ jetstream.Msg) error { return nil })
	if err != nil {
		t.Fatalf("consume alias: %v", err)
	}
	cons.Stop()
}

func TestErrTerminateTerminatedOnJS(t *testing.T) {
	assertSentinelHandled(t, func() error {
		return fmt.Errorf("bad payload: %w", ErrTerminate)
	}, "S_TERM", "s_term.>", "c_term")
}

func TestErrSkipAckedOnJS(t *testing.T) {
	assertSentinelHandled(t, func() error { return ErrSkip }, "S_SKIP", "s_skip.>", "c_skip")
}

// assertSentinelHandled publishes a single message, runs a handler that
// returns the given sentinel error, and verifies the message is not
// redelivered - handler called exactly once, no pending.
func assertSentinelHandled(t *testing.T, returnErr func() error, stream, subjectPattern, consumer string) {
	t.Helper()
	s := runJSServer(t)
	c := mustConnect(t, s.ClientURL(), Config{Workers: 1})
	j := mustJetStream(t, c)
	ensureStream(t, j, stream, subjectPattern)

	subj := strings.TrimSuffix(subjectPattern, ".>") + ".x"
	if _, err := j.Publish(context.Background(), subj, []byte("x")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	var calls atomic.Int32
	cons, err := j.NewConsumer(context.Background(), stream, jetstream.ConsumerConfig{
		Durable:       consumer,
		FilterSubject: subjectPattern,
		AckWait:       2 * time.Second,
	}, func(_ context.Context, _ jetstream.Msg) error {
		calls.Add(1)
		return returnErr()
	})
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer cons.Stop()

	// Wait long enough that, if Nak'd, the server would have redelivered.
	time.Sleep(300 * time.Millisecond)

	if got := calls.Load(); got != 1 {
		t.Fatalf("handler called %d times, want 1 (sentinel should ack, not redeliver)", got)
	}

	stream2, err := j.Raw().Stream(context.Background(), stream)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	consInfo, err := stream2.Consumer(context.Background(), consumer)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	info, err := consInfo.Info(context.Background())
	if err != nil {
		t.Fatalf("info: %v", err)
	}
	if info.NumAckPending != 0 || info.NumPending != 0 {
		t.Fatalf("ackPending=%d pending=%d, want 0/0", info.NumAckPending, info.NumPending)
	}
}

// dispatchStub is a jetstream.Msg stub that records Ack/Nak calls so we
// can verify DispatchJet's behavior directly, without spinning up a real
// JS consumer.
type dispatchStub struct {
	jetstream.Msg
	mu       sync.Mutex
	acked    int
	naked    int
	termed   int
	ackDelay time.Duration // optional sleep inside Ack (for ctx-expiry test)
}

func (s *dispatchStub) Ack() error {
	if s.ackDelay > 0 {
		time.Sleep(s.ackDelay)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acked++
	return nil
}

func (s *dispatchStub) Nak() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.naked++
	return nil
}

func (s *dispatchStub) Term() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.termed++
	return nil
}

func (s *dispatchStub) snapshot() (int, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.acked, s.naked, s.termed
}

func TestDispatchJetDirect(t *testing.T) {
	t.Run("nil_error_acks", func(t *testing.T) {
		stub := &dispatchStub{}
		h := func(_ context.Context, _ jetstream.Msg) error { return nil }
		DispatchJet(context.Background(), h, stub, time.Second)
		a, n, term := stub.snapshot()
		if a != 1 || n != 0 || term != 0 {
			t.Fatalf("acks=%d naks=%d terms=%d, want 1 0 0", a, n, term)
		}
	})

	t.Run("plain_error_naks", func(t *testing.T) {
		stub := &dispatchStub{}
		h := func(_ context.Context, _ jetstream.Msg) error { return errors.New("boom") }
		DispatchJet(context.Background(), h, stub, time.Second)
		a, n, term := stub.snapshot()
		if a != 0 || n != 1 || term != 0 {
			t.Fatalf("acks=%d naks=%d terms=%d, want 0 1 0", a, n, term)
		}
	})

	t.Run("err_terminate_terms", func(t *testing.T) {
		stub := &dispatchStub{}
		h := func(_ context.Context, _ jetstream.Msg) error {
			return fmt.Errorf("bad: %w", ErrTerminate)
		}
		DispatchJet(context.Background(), h, stub, time.Second)
		a, n, term := stub.snapshot()
		if a != 0 || n != 0 || term != 1 {
			t.Fatalf("acks=%d naks=%d terms=%d, want 0 0 1", a, n, term)
		}
	})

	t.Run("err_skip_acks", func(t *testing.T) {
		stub := &dispatchStub{}
		h := func(_ context.Context, _ jetstream.Msg) error { return ErrSkip }
		DispatchJet(context.Background(), h, stub, time.Second)
		a, n, term := stub.snapshot()
		if a != 1 || n != 0 || term != 0 {
			t.Fatalf("acks=%d naks=%d terms=%d, want 1 0 0", a, n, term)
		}
	})

	t.Run("ctx_expired_no_action", func(t *testing.T) {
		stub := &dispatchStub{}
		// Handler observes its ctx expiring and returns its err; dispatch
		// sees ctx.Err() != nil and does nothing (neither Ack nor Nak).
		h := func(ctx context.Context, _ jetstream.Msg) error {
			<-ctx.Done()
			return ctx.Err()
		}
		DispatchJet(context.Background(), h, stub, 10*time.Millisecond)
		a, n, term := stub.snapshot()
		if a != 0 || n != 0 || term != 0 {
			t.Fatalf("acks=%d naks=%d terms=%d, want 0 0 0 (ctx expiry -> no ack action)", a, n, term)
		}
	})
}

func TestPerMessageTimeoutDefaults(t *testing.T) {
	cases := []struct {
		ackWait, buffer, want time.Duration
	}{
		{ackWait: 1 * time.Second, buffer: 0, want: 900 * time.Millisecond},                            // default buffer = 10%
		{ackWait: 10 * time.Second, buffer: 2 * time.Second, want: 8 * time.Second},                    // explicit buffer
		{ackWait: 100 * time.Millisecond, buffer: 200 * time.Millisecond, want: 50 * time.Millisecond}, // buffer capped at ackWait/2
		{ackWait: 0, buffer: 0, want: defaultAckWait - defaultAckWait/10},                              // ackWait defaults to 30s
	}
	for _, tc := range cases {
		got := perMessageTimeout(tc.ackWait, tc.buffer)
		if got != tc.want {
			t.Errorf("perMessageTimeout(%v,%v) = %v, want %v", tc.ackWait, tc.buffer, got, tc.want)
		}
	}
}
