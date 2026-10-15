package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// JSONHandler wraps a typed handler for Core NATS. It decodes m.Data as JSON
// into T and passes the decoded payload to h. If json.Unmarshal fails, it
// returns an error joined with ErrTerminate, signaling that the malformed payload
// cannot be processed and should not be retried.
func JSONHandler[T any](h func(context.Context, *natsio.Msg, T) error) Handler {
	return func(ctx context.Context, m *natsio.Msg) error {
		var payload T
		var data []byte
		if m != nil {
			data = m.Data
		}
		if err := json.Unmarshal(data, &payload); err != nil {
			return fmt.Errorf("json unmarshal (%T): %w", payload, errors.Join(err, ErrTerminate))
		}
		return h(ctx, m, payload)
	}
}

// JetJSONHandler wraps a typed handler for JetStream. It decodes m.Data() as JSON
// into T and passes the decoded payload to h. If json.Unmarshal fails, it
// returns an error joined with ErrTerminate, causing JetStream to Term the poison
// message instead of redelivering indefinitely.
func JetJSONHandler[T any](h func(context.Context, jetstream.Msg, T) error) JetHandler {
	return func(ctx context.Context, m jetstream.Msg) error {
		var payload T
		var data []byte
		if m != nil {
			data = m.Data()
		}
		if err := json.Unmarshal(data, &payload); err != nil {
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
