// Smoke: connect to a real NATS server, round-trip a handful of core +
// JetStream messages, exit 0 on success, non-zero on any failure.
//
//	./test/setup.sh        # in another terminal
//	go run ./test/smoke
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	qpnats "github.com/rlsvr/hirnok/pkg/nats"
)

const (
	defaultURL = "nats://localhost:4222"
	timeout    = 10 * time.Second
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "smoke: FAIL: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("smoke: OK")
}

func run() error {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = defaultURL
	}

	conn, err := qpnats.Connect(qpnats.Config{
		URL:       url,
		Name:      "hirnok-smoke",
		QueueSize: 32,
		Workers:   2,
	})
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()
	log.Println("connected")

	if err := coreRoundTrip(conn); err != nil {
		return fmt.Errorf("core: %w", err)
	}
	if err := jetRoundTrip(conn); err != nil {
		return fmt.Errorf("jetstream: %w", err)
	}
	return nil
}

func coreRoundTrip(conn *qpnats.Conn) error {
	const total = 20
	received := make(chan *natsio.Msg, total)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	sub, err := conn.Subscribe(ctx, "smoke.core.*", func(_ context.Context, m *natsio.Msg) error {
		received <- m
		return nil
	})
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	defer sub.Unsubscribe() //nolint:errcheck

	for i := range total {
		if err := conn.Publish(fmt.Sprintf("smoke.core.%d", i), fmt.Appendf(nil, "msg-%d", i)); err != nil {
			return fmt.Errorf("publish: %w", err)
		}
	}

	want := make(map[string]bool, total)
	for i := range total {
		want[fmt.Sprintf("msg-%d", i)] = true
	}

	deadline := time.After(timeout)
	for range total {
		select {
		case m := <-received:
			body := string(m.Data)
			if !want[body] {
				return fmt.Errorf("unexpected or duplicate body: %q", body)
			}
			delete(want, body)
		case <-deadline:
			return fmt.Errorf("missing %d core messages", len(want))
		}
	}
	if len(want) != 0 {
		return fmt.Errorf("missing %d core messages after drain", len(want))
	}
	log.Printf("core: %d / %d round-tripped", total, total)
	return nil
}

func jetRoundTrip(conn *qpnats.Conn) error {
	const total = 20
	js, err := conn.JetStream()
	if err != nil {
		return fmt.Errorf("jetstream context: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if _, err := js.Raw().CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "SMOKE",
		Subjects: []string{"smoke.js.>"},
	}); err != nil {
		return fmt.Errorf("create stream: %w", err)
	}

	for i := range total {
		if _, err := js.Publish(ctx, "smoke.js.x", fmt.Appendf(nil, "js-%d", i)); err != nil {
			return fmt.Errorf("publish %d: %w", i, err)
		}
	}

	var seen atomic.Int32
	cons, err := js.NewConsumer(ctx, "SMOKE", jetstream.ConsumerConfig{
		Durable:       "smoke-consumer",
		FilterSubject: "smoke.js.>",
		AckWait:       5 * time.Second,
	}, func(_ context.Context, _ jetstream.Msg) error {
		seen.Add(1)
		return nil
	})
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	defer cons.Stop()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if seen.Load() >= total {
			log.Printf("jetstream: %d / %d round-tripped", seen.Load(), total)
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("only received %d / %d jetstream messages", seen.Load(), total)
}
