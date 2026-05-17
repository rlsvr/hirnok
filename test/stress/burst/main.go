// Burst/spike: subscriber sits idle, then a burst of N messages arrives
// at once. Measures cold-start latency (first message → handler) and
// drain time (all N processed).
//
//	./test/setup.sh                              # in another terminal
//	go run ./test/stress/burst --burst 50000 --workers 4
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	natsio "github.com/nats-io/nats.go"

	qpnats "github.com/rlsvr/hirnok/pkg/nats"
)

func main() {
	var (
		url       = flag.String("url", envOr("NATS_URL", "nats://localhost:4222"), "NATS URL")
		workers   = flag.Int("workers", 4, "consumer worker goroutines")
		queueSize = flag.Int("queue-size", 64, "per-subscription queue buffer")
		burst     = flag.Int("burst", 50000, "burst size")
		idle      = flag.Duration("idle", 500*time.Millisecond, "idle period before burst")
		payload   = flag.Int("payload", 64, "payload size in bytes")
	)
	flag.Parse()

	if err := run(*url, *workers, *queueSize, *burst, *idle, *payload); err != nil {
		fmt.Fprintf(os.Stderr, "stress/burst: %v\n", err)
		os.Exit(1)
	}
}

func run(url string, workers, queueSize, burst int, idle time.Duration, payloadSize int) error {
	conn, err := qpnats.Connect(qpnats.Config{
		URL:       url,
		Name:      "hirnok-stress-burst",
		QueueSize: queueSize,
		Workers:   workers,
	})
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	var received atomic.Int64
	var firstAt atomic.Int64

	sub, err := conn.Subscribe(context.Background(), "stress.burst", func(_ context.Context, _ *natsio.Msg) error {
		if received.Add(1) == 1 {
			firstAt.Store(time.Now().UnixNano())
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	defer sub.Unsubscribe() //nolint:errcheck

	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}

	log.Printf("workers=%d queue=%d burst=%d idle=%v payload=%dB", workers, queueSize, burst, idle, payloadSize)
	log.Printf("subscriber idle for %v ...", idle)
	time.Sleep(idle)

	log.Printf("firing burst of %d ...", burst)
	burstStart := time.Now()
	for range burst {
		if err := conn.Publish("stress.burst", payload); err != nil {
			return fmt.Errorf("publish: %w", err)
		}
	}
	publishElapsed := time.Since(burstStart)

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if received.Load() >= int64(burst) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	drainElapsed := time.Since(burstStart)
	got := received.Load()

	firstLatency := time.Duration(0)
	if firstAt.Load() != 0 {
		firstLatency = time.Unix(0, firstAt.Load()).Sub(burstStart)
	}

	fmt.Println()
	fmt.Println("results:")
	fmt.Printf("  burst size:        %d\n", burst)
	fmt.Printf("  received:          %d\n", got)
	fmt.Printf("  publish time:      %v\n", publishElapsed.Round(time.Millisecond))
	fmt.Printf("  drain time:        %v\n", drainElapsed.Round(time.Millisecond))
	fmt.Printf("  first-msg latency: %v\n", firstLatency.Round(time.Microsecond))
	fmt.Printf("  drain throughput:  %.0f msgs/sec\n", float64(got)/drainElapsed.Seconds())

	if got != int64(burst) {
		return fmt.Errorf("only received %d / %d", got, burst)
	}
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
