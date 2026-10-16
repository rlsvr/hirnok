package nats

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// JSONConfig holds configuration for JSON decoding in JSONHandler and JetJSONHandler.
type JSONConfig struct {
	DisallowUnknown bool
	CustomDecoder   func([]byte, any) error
}

// JSONOption configures JSON decoding behavior.
type JSONOption func(*JSONConfig)

// WithDisallowUnknownFields configures the decoder to reject payloads containing
// fields not present in the target struct.
func WithDisallowUnknownFields() JSONOption {
	return func(c *JSONConfig) {
		c.DisallowUnknown = true
	}
}

// WithCustomDecoder sets a custom decoder function for unmarshaling JSON bytes.
func WithCustomDecoder(fn func([]byte, any) error) JSONOption {
	return func(c *JSONConfig) {
		c.CustomDecoder = fn
	}
}

func buildJSONConfig(opts []JSONOption) JSONConfig {
	var cfg JSONConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

func decodeJSONPayload[T any](data []byte, payload *T, cfg JSONConfig) error {
	if cfg.CustomDecoder != nil {
		return cfg.CustomDecoder(data, payload)
	}
	if cfg.DisallowUnknown {
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.DisallowUnknownFields()
		if err := dec.Decode(payload); err != nil {
			return err
		}
		var extra any
		if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
			if err == nil {
				return errors.New("json: unexpected trailing data")
			}
			return err
		}
		return nil
	}
	return json.Unmarshal(data, payload)
}

// JSONHandler wraps a typed handler for Core NATS. It decodes m.Data as JSON
// into T and passes the decoded payload to h. If decoding fails, it
// returns an error joined with ErrTerminate, signaling that the malformed payload
// cannot be processed and should not be retried.
func JSONHandler[T any](h func(context.Context, *natsio.Msg, T) error, opts ...JSONOption) Handler {
	if h == nil {
		panic("nats: JSONHandler requires a non-nil handler")
	}
	cfg := buildJSONConfig(opts)
	return func(ctx context.Context, m *natsio.Msg) error {
		var payload T
		var data []byte
		if m != nil {
			data = m.Data
		}
		if err := decodeJSONPayload(data, &payload, cfg); err != nil {
			return fmt.Errorf("json unmarshal (%T): %w", payload, errors.Join(err, ErrTerminate))
		}
		return h(ctx, m, payload)
	}
}

// JetJSONHandler wraps a typed handler for JetStream. It decodes m.Data() as JSON
// into T and passes the decoded payload to h. If decoding fails, it
// returns an error joined with ErrTerminate, causing JetStream to Term the poison
// message instead of redelivering indefinitely.
func JetJSONHandler[T any](h func(context.Context, jetstream.Msg, T) error, opts ...JSONOption) JetHandler {
	if h == nil {
		panic("nats: JetJSONHandler requires a non-nil handler")
	}
	cfg := buildJSONConfig(opts)
	return func(ctx context.Context, m jetstream.Msg) error {
		var payload T
		var data []byte
		if m != nil {
			data = m.Data()
		}
		if err := decodeJSONPayload(data, &payload, cfg); err != nil {
			return fmt.Errorf("json unmarshal (%T): %w", payload, errors.Join(err, ErrTerminate))
		}
		return h(ctx, m, payload)
	}
}

// PublishJSON marshals v as JSON and publishes it to subject.
func (c *Conn) PublishJSON(subject string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("nats marshal (%T): %w", v, err)
	}
	return c.Publish(subject, data)
}

// PublishJSON marshals v as JSON and publishes it to subject via JetStream.
func (j *JetStream) PublishJSON(ctx context.Context, subject string, v any) (*jetstream.PubAck, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("js marshal (%T): %w", v, err)
	}
	return j.Publish(ctx, subject, data)
}
