package orionmesh

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Queue is the pub/sub helper for a single named queue. Get one via
// Client.Queue(name); it lazily loads the underlying Queue resource on
// first use.
type Queue struct {
	client *Client
	name   string
	spec   map[string]any
}

func (q *Queue) Name() string { return q.name }

// Refresh re-loads the Queue resource from the controller.
func (q *Queue) Refresh() error {
	r, err := q.client.Get("Queue", q.name)
	if err != nil {
		var notFound *ResourceNotFoundError
		if errors.As(err, &notFound) {
			return &QueueNotFoundError{Name: q.name}
		}
		return err
	}
	q.spec = r.Spec
	return nil
}

func (q *Queue) ensureSpec() error {
	if q.spec != nil { return nil }
	return q.Refresh()
}

// Subject returns spec.subject if set, otherwise orion.queue.<name>.
func (q *Queue) Subject() (string, error) {
	if err := q.ensureSpec(); err != nil { return "", err }
	if s, ok := q.spec["subject"].(string); ok && s != "" {
		return s, nil
	}
	return "orion.queue." + q.name, nil
}

// Stream returns spec.stream if set, otherwise ORION_QUEUE_<NAME>.
func (q *Queue) Stream() (string, error) {
	if err := q.ensureSpec(); err != nil { return "", err }
	if s, ok := q.spec["stream"].(string); ok && s != "" {
		return s, nil
	}
	return "ORION_QUEUE_" + strings.ToUpper(strings.ReplaceAll(q.name, "-", "_")), nil
}

// Type returns spec.type (default "work").
func (q *Queue) Type() (string, error) {
	if err := q.ensureSpec(); err != nil { return "work", err }
	if t, ok := q.spec["type"].(string); ok && t != "" {
		return t, nil
	}
	return "work", nil
}

// Pub publishes one message. Value is JSON-encoded unless it's already
// a []byte or string. Returns the JetStream sequence number.
func (q *Queue) Pub(value any) (uint64, error) {
	js, err := q.client.nats()
	if err != nil { return 0, err }
	subj, err := q.Subject()
	if err != nil { return 0, err }
	stream, err := q.Stream()
	if err != nil { return 0, err }
	if err := q.ensureStream(js, stream, subj); err != nil { return 0, err }
	payload, err := toBytes(value)
	if err != nil { return 0, err }
	ack, err := js.Publish(context.Background(), subj, payload)
	if err != nil { return 0, err }
	return ack.Sequence, nil
}

// PubMany publishes a batch and returns the count.
func (q *Queue) PubMany(values []any) (int, error) {
	js, err := q.client.nats()
	if err != nil { return 0, err }
	subj, err := q.Subject()
	if err != nil { return 0, err }
	stream, err := q.Stream()
	if err != nil { return 0, err }
	if err := q.ensureStream(js, stream, subj); err != nil { return 0, err }
	n := 0
	for _, v := range values {
		b, err := toBytes(v)
		if err != nil { return n, err }
		if _, err := js.Publish(context.Background(), subj, b); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// Sub subscribes and returns a channel that yields decoded rows. Pass
// limit > 0 to stop after N messages; pass 0 to run until ctx is cancelled.
//
// The group is the JetStream durable consumer name; for work queues
// sharing it load-balances. For topic queues each subscriber should
// use a unique name.
func (q *Queue) Sub(ctx context.Context, group string, limit int) (<-chan map[string]any, <-chan error) {
	out := make(chan map[string]any)
	errCh := make(chan error, 1)
	go func() {
		defer close(out)
		js, err := q.client.nats()
		if err != nil { errCh <- err; return }
		subj, err := q.Subject()
		if err != nil { errCh <- err; return }
		stream, err := q.Stream()
		if err != nil { errCh <- err; return }
		if err := q.ensureStream(js, stream, subj); err != nil { errCh <- err; return }

		if group == "" {
			t, _ := q.Type()
			if t == "topic" {
				group = fmt.Sprintf("%s-go-%d", q.name, os.Getpid())
			} else {
				group = q.name + "-go-workers"
			}
		}
		strm, err := js.Stream(ctx, stream)
		if err != nil { errCh <- err; return }
		consumer, err := strm.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
			Durable:       group,
			FilterSubject: subj,
			AckPolicy:     jetstream.AckExplicitPolicy,
		})
		if err != nil { errCh <- err; return }

		delivered := 0
		for {
			if limit > 0 && delivered >= limit { return }
			select {
			case <-ctx.Done(): return
			default:
			}
			batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
			if err != nil { errCh <- err; return }
			gotOne := false
			for msg := range batch.Messages() {
				var row map[string]any
				if err := json.Unmarshal(msg.Data(), &row); err != nil {
					row = map[string]any{"_raw": string(msg.Data())}
				}
				if _, ok := row["_subject"]; !ok {
					row["_subject"] = msg.Subject()
				}
				select {
				case out <- row:
				case <-ctx.Done(): return
				}
				if err := msg.Ack(); err != nil { errCh <- err; return }
				delivered++
				gotOne = true
				if limit > 0 && delivered >= limit { return }
			}
			if !gotOne && limit > 0 {
				return // dry queue + bounded run = stop
			}
		}
	}()
	return out, errCh
}

func (q *Queue) ensureStream(js jetstream.JetStream, stream, subj string) error {
	ctx := context.Background()
	_, err := js.Stream(ctx, stream)
	if err == nil { return nil }
	// Stream missing — create.
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     stream,
		Subjects: []string{subj},
	})
	return err
}

func toBytes(v any) ([]byte, error) {
	switch x := v.(type) {
	case []byte: return x, nil
	case string: return []byte(x), nil
	default:    return json.Marshal(v)
	}
}
