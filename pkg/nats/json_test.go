package nats

import (
	"context"
	"errors"
	"testing"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type samplePayload struct {
	ID  string `json:"id"`
	Val int    `json:"val"`
}

const testJSONSubject = "test.json"

func TestJSONHandler(t *testing.T) {
	t.Parallel()

	t.Run("valid json", func(t *testing.T) {
		t.Parallel()
		called := false
		handler := JSONHandler[samplePayload](func(_ context.Context, msg *natsio.Msg, p samplePayload) error {
			called = true
			if p.ID != "abc" || p.Val != 42 {
				t.Fatalf("unexpected payload: %+v", p)
			}
			if msg.Subject != testJSONSubject {
				t.Fatalf("unexpected subject: %s", msg.Subject)
			}
			return nil
		})

		msg := &natsio.Msg{
			Subject: testJSONSubject,
			Data:    []byte(`{"id":"abc","val":42}`),
		}
		if err := handler(context.Background(), msg); err != nil {
			t.Fatalf("handler returned unexpected error: %v", err)
		}
		if !called {
			t.Fatal("expected handler to be invoked")
		}
	})

	t.Run("invalid json returns ErrTerminate", func(t *testing.T) {
		t.Parallel()
		called := false
		handler := JSONHandler[samplePayload](func(_ context.Context, _ *natsio.Msg, _ samplePayload) error {
			called = true
			return nil
		})

		msg := &natsio.Msg{
			Subject: testJSONSubject,
			Data:    []byte(`not-json`),
		}
		err := handler(context.Background(), msg)
		if err == nil {
			t.Fatal("expected error for malformed json")
		}
		if !errors.Is(err, ErrTerminate) {
			t.Fatalf("expected ErrTerminate, got: %v", err)
		}
		if called {
			t.Fatal("handler should not have been called on unmarshal error")
		}
	})

	t.Run("inner handler error propagated", func(t *testing.T) {
		t.Parallel()
		sentinel := errors.New("business rule violation")
		handler := JSONHandler[samplePayload](func(_ context.Context, _ *natsio.Msg, _ samplePayload) error {
			return sentinel
		})

		msg := &natsio.Msg{Data: []byte(`{"id":"1"}`)}
		err := handler(context.Background(), msg)
		if !errors.Is(err, sentinel) {
			t.Fatalf("expected sentinel, got %v", err)
		}
	})

	t.Run("disallow unknown fields rejects unexpected keys", func(t *testing.T) {
		t.Parallel()
		handler := JSONHandler[samplePayload](func(_ context.Context, _ *natsio.Msg, _ samplePayload) error {
			t.Fatal("handler should not be called when unknown fields present")
			return nil
		}, WithDisallowUnknownFields())

		msg := &natsio.Msg{Data: []byte(`{"id":"1","val":10,"unexpected_extra":"bad"}`)}
		err := handler(context.Background(), msg)
		if err == nil {
			t.Fatal("expected error for payload with unknown fields")
		}
		if !errors.Is(err, ErrTerminate) {
			t.Fatalf("expected ErrTerminate, got: %v", err)
		}
	})

	t.Run("custom decoder option", func(t *testing.T) {
		t.Parallel()
		customCalled := false
		handler := JSONHandler[samplePayload](func(_ context.Context, _ *natsio.Msg, p samplePayload) error {
			if p.ID != "custom-id" {
				t.Fatalf("unexpected id: %s", p.ID)
			}
			return nil
		}, WithCustomDecoder(func(_ []byte, v any) error {
			customCalled = true
			p, ok := v.(*samplePayload)
			if !ok {
				t.Fatalf("unexpected target type: %T", v)
			}
			p.ID = "custom-id"
			return nil
		}))

		msg := &natsio.Msg{Data: []byte(`{}`)}
		if err := handler(context.Background(), msg); err != nil {
			t.Fatalf("unexpected handler error: %v", err)
		}
		if !customCalled {
			t.Fatal("custom decoder was not invoked")
		}
	})
}

func TestJetJSONHandler(t *testing.T) {
	t.Parallel()

	srv := runJSServer(t)
	c := mustConnect(t, srv.ClientURL(), Config{})
	js, err := c.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	ensureStream(t, js, "JSON_STREAM", "json.jet.*")

	t.Run("valid json and invalid json dispatch", func(t *testing.T) {
		validCh := make(chan samplePayload, 1)

		handler := JetJSONHandler[samplePayload](func(_ context.Context, _ jetstream.Msg, p samplePayload) error {
			validCh <- p
			return nil
		})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cfg := DefaultConsumerConfig("json_test_sub")
		cfg.FilterSubject = "json.jet.*"
		_, err := js.NewConsumer(ctx, "JSON_STREAM", cfg, handler)
		if err != nil {
			t.Fatalf("new consumer: %v", err)
		}

		// Publish valid payload
		_, err = js.Publish(ctx, "json.jet.valid", []byte(`{"id":"jet1","val":99}`))
		if err != nil {
			t.Fatalf("publish: %v", err)
		}

		select {
		case p := <-validCh:
			if p.ID != "jet1" || p.Val != 99 {
				t.Fatalf("unexpected payload: %+v", p)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for valid payload")
		}

		// Also check that invalid payload directly through JetJSONHandler returns ErrTerminate
		fakeMsg := &testFakeJetMsg{data: []byte(`broken json`)}
		invErr := handler(ctx, fakeMsg)
		if invErr == nil || !errors.Is(invErr, ErrTerminate) {
			t.Fatalf("expected ErrTerminate, got: %v", invErr)
		}
	})
}

func TestPublishJSON(t *testing.T) {
	t.Parallel()

	srv := runJSServer(t)
	c := mustConnect(t, srv.ClientURL(), Config{})
	js, err := c.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	ensureStream(t, js, "PUB_STREAM", "pub.json.*")

	t.Run("core publish json", func(t *testing.T) {
		recvCh := make(chan samplePayload, 1)
		_, err := c.Subscribe(context.Background(), "pub.json.core",
			JSONHandler[samplePayload](func(_ context.Context, _ *natsio.Msg, p samplePayload) error {
				recvCh <- p
				return nil
			}),
			WithQueueSize(10),
		)
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}

		payload := samplePayload{ID: "core-1", Val: 123}
		if err := c.PublishJSON("pub.json.core", payload); err != nil {
			t.Fatalf("publish json: %v", err)
		}

		select {
		case got := <-recvCh:
			if got != payload {
				t.Fatalf("got %+v, want %+v", got, payload)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for published msg")
		}
	})

	t.Run("core publish json invalid type", func(t *testing.T) {
		unmarshalable := make(chan int)
		if err := c.PublishJSON("pub.json.invalid", unmarshalable); err == nil {
			t.Fatal("expected marshal error for channel type")
		}
	})

	t.Run("jetstream publish json", func(t *testing.T) {
		ctx := context.Background()
		payload := samplePayload{ID: "js-1", Val: 456}
		ack, err := js.PublishJSON(ctx, "pub.json.js", payload)
		if err != nil {
			t.Fatalf("publish json js: %v", err)
		}
		if ack == nil || ack.Stream != "PUB_STREAM" {
			t.Fatalf("unexpected ack: %+v", ack)
		}
	})

	t.Run("jetstream publish json invalid type", func(t *testing.T) {
		unmarshalable := make(chan int)
		if _, err := js.PublishJSON(context.Background(), "pub.json.invalid", unmarshalable); err == nil {
			t.Fatal("expected marshal error for channel type")
		}
	})
}

// testFakeJetMsg implements jetstream.Msg minimal interface for unit testing
type testFakeJetMsg struct {
	jetstream.Msg
	data []byte
}

func (m *testFakeJetMsg) Data() []byte {
	return m.data
}

func TestJSONHandler_NilHandlerPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil handler in JSONHandler")
		}
	}()
	_ = JSONHandler[samplePayload](nil)
}

func TestJetJSONHandler_NilHandlerPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil handler in JetJSONHandler")
		}
	}()
	_ = JetJSONHandler[samplePayload](nil)
}
