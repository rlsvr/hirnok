package nats

import (
	"context"
	"fmt"
	"testing"
	"time"

	natsio "github.com/nats-io/nats.go"
)

func TestSubscribeChan_Basic(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	c := mustConnect(t, s.ClientURL(), Config{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	subject := "test.chan.basic"
	cs, err := SubscribeChan(ctx, c, subject, 16)
	if err != nil {
		t.Fatalf("subscribe chan: %v", err)
	}
	if cs.Sub() == nil {
		t.Fatal("expected non-nil inner sub")
	}

	const total = 5
	for i := range total {
		if err := c.Publish(subject, []byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	for i := range total {
		select {
		case msg, ok := <-cs.C():
			if !ok {
				t.Fatalf("channel closed early at msg %d", i)
			}
			expected := fmt.Sprintf("msg-%d", i)
			if string(msg.Data) != expected {
				t.Fatalf("got %q, want %q", string(msg.Data), expected)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for msg %d", i)
		}
	}

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer drainCancel()

	if err := cs.Drain(drainCtx); err != nil {
		t.Fatalf("drain: %v", err)
	}

	// Channel should now be closed
	select {
	case _, ok := <-cs.C():
		if ok {
			t.Fatal("expected channel to be closed after drain")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for channel closure")
	}
}

func TestSubscribeChanJSON_Basic(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	c := mustConnect(t, s.ClientURL(), Config{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	subject := "test.chan.json"
	cs, err := SubscribeChanJSON[samplePayload](ctx, c, subject, 16)
	if err != nil {
		t.Fatalf("subscribe chan json: %v", err)
	}

	// 1. Valid payload
	if err := c.PublishJSON(subject, samplePayload{ID: "first", Val: 10}); err != nil {
		t.Fatalf("publish 1: %v", err)
	}
	// 2. Corrupted JSON (should be skipped)
	if err := c.Publish(subject, []byte(`broken json`)); err != nil {
		t.Fatalf("publish broken: %v", err)
	}
	// 3. Second valid payload
	if err := c.PublishJSON(subject, samplePayload{ID: "second", Val: 20}); err != nil {
		t.Fatalf("publish 2: %v", err)
	}

	select {
	case p := <-cs.C():
		if p.ID != "first" || p.Val != 10 {
			t.Fatalf("unexpected first: %+v", p)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for first payload")
	}

	select {
	case p := <-cs.C():
		if p.ID != "second" || p.Val != 20 {
			t.Fatalf("unexpected second: %+v", p)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for second payload")
	}

	if err := cs.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()
	if err := cs.Wait(waitCtx); err != nil {
		t.Fatalf("wait: %v", err)
	}

	// Channel should be closed
	select {
	case _, ok := <-cs.C():
		if ok {
			t.Fatal("expected channel to be closed after stop/wait")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for channel closure")
	}
}

func TestSubscribeChan_ContextCancellation(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	c := mustConnect(t, s.ClientURL(), Config{})

	ctx, cancel := context.WithCancel(context.Background())
	subject := "test.chan.cancel"

	cs, err := SubscribeChan(ctx, c, subject, 8)
	if err != nil {
		t.Fatalf("subscribe chan: %v", err)
	}

	// Cancel context to stop subscription
	cancel()

	select {
	case <-cs.Stopped():
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for subscription to stop")
	}

	// Verify channel closed
	select {
	case _, ok := <-cs.C():
		if ok {
			t.Fatal("expected channel to be closed after context cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for channel closure")
	}
}

func TestSubscribeChan_ValidationErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	if _, err := SubscribeChan(ctx, nil, "subj", 10); err == nil {
		t.Fatal("expected error on nil conn")
	}

	s := runServer(t)
	c := mustConnect(t, s.ClientURL(), Config{})

	if _, err := SubscribeChan(ctx, c, "", 10); err == nil {
		t.Fatal("expected error on empty subject")
	}

	if _, err := SubscribeChanTransform[string](ctx, c, "subj", 10, nil); err == nil {
		t.Fatal("expected error on nil transform")
	}
}

func TestSubscribeChanTransform(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	c := mustConnect(t, s.ClientURL(), Config{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	subject := "test.chan.transform"
	// Transform only keeps messages starting with "keep:"
	cs, err := SubscribeChanTransform(ctx, c, subject, 8, func(_ context.Context, m *natsio.Msg) (string, bool, error) {
		str := string(m.Data)
		if len(str) >= 5 && str[:5] == "keep:" {
			return str[5:], true, nil
		}
		return "", false, nil
	})
	if err != nil {
		t.Fatalf("subscribe transform: %v", err)
	}

	_ = c.Publish(subject, []byte("drop:1"))
	_ = c.Publish(subject, []byte("keep:hello"))
	_ = c.Publish(subject, []byte("drop:2"))
	_ = c.Publish(subject, []byte("keep:world"))

	select {
	case val := <-cs.C():
		if val != "hello" {
			t.Fatalf("got %q, want hello", val)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for hello")
	}

	select {
	case val := <-cs.C():
		if val != "world" {
			t.Fatalf("got %q, want world", val)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for world")
	}

	_ = cs.Unsubscribe()
}
