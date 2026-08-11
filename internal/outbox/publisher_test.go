package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/eztwokey/wb-orders/internal/event"
	"github.com/google/uuid"
)

type fakeRepository struct {
	events   []Event
	mu       sync.Mutex
	marked   []uuid.UUID
	released []uuid.UUID
	failed   []uuid.UUID
}

func (r *fakeRepository) Claim(context.Context, string, int, time.Duration) ([]Event, error) {
	return append([]Event(nil), r.events...), nil
}
func (r *fakeRepository) MarkPublished(_ context.Context, id uuid.UUID, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.marked = append(r.marked, id)
	return nil
}
func (r *fakeRepository) Release(_ context.Context, id uuid.UUID, _, _ string, _ time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.released = append(r.released, id)
	return nil
}
func (r *fakeRepository) MarkFailed(_ context.Context, id uuid.UUID, _, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failed = append(r.failed, id)
	return nil
}
func (r *fakeRepository) PendingCount(context.Context) (int64, error) {
	return int64(len(r.events)), nil
}

type fakeProducer struct{ err error }

func (p fakeProducer) Publish(context.Context, string, string, []byte, event.Envelope) error {
	return p.err
}

func TestPublisherMarksSuccessfulEvent(t *testing.T) {
	t.Parallel()
	envelope, err := event.New(event.OrderCreated, uuid.New(), "publisher-test", map[string]string{"value": "ok"})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := envelopeJSON(envelope)
	repository := &fakeRepository{events: []Event{{ID: envelope.EventID, AggregateID: envelope.AggregateID, EventType: envelope.EventType, Payload: raw, Attempts: 1}}}
	publisher := NewPublisher(repository, fakeProducer{}, PublisherConfig{
		Owner: "test", OrderCreatedTopic: "wb.orders.created", OrderStatusChangedTopic: "wb.orders.status-changed",
		Workers: 1, BatchSize: 1, PollInterval: time.Second, LeaseDuration: time.Minute,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := publisher.publishBatch(context.Background()); err != nil {
		t.Fatalf("publishBatch() error = %v", err)
	}
	if len(repository.marked) != 1 || repository.marked[0] != envelope.EventID {
		t.Fatalf("marked = %v, want event id", repository.marked)
	}
}

func TestPublisherReleasesFailedEvent(t *testing.T) {
	t.Parallel()
	envelope, _ := event.New(event.OrderCreated, uuid.New(), "publisher-test", map[string]string{"value": "ok"})
	raw, _ := envelopeJSON(envelope)
	repository := &fakeRepository{events: []Event{{ID: envelope.EventID, AggregateID: envelope.AggregateID, EventType: envelope.EventType, Payload: raw, Attempts: 2}}}
	publisher := NewPublisher(repository, fakeProducer{err: errors.New("kafka unavailable")}, PublisherConfig{
		Owner: "test", OrderCreatedTopic: "wb.orders.created", OrderStatusChangedTopic: "wb.orders.status-changed",
		Workers: 1, BatchSize: 1, PollInterval: time.Second, LeaseDuration: time.Minute,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := publisher.publishBatch(context.Background()); err == nil {
		t.Fatal("publishBatch() error = nil, want failure")
	}
	if len(repository.released) != 1 {
		t.Fatalf("released = %v, want one event", repository.released)
	}
}

func TestPublisherPermanentlyFailsInvalidEnvelope(t *testing.T) {
	t.Parallel()
	id := uuid.New()
	repository := &fakeRepository{events: []Event{{
		ID: id, AggregateID: uuid.New(), EventType: event.OrderCreated, Payload: []byte(`{"event_id":"invalid"}`), Attempts: 1,
	}}}
	publisher := NewPublisher(repository, fakeProducer{}, PublisherConfig{
		Owner: "test", OrderCreatedTopic: "wb.orders.created", OrderStatusChangedTopic: "wb.orders.status-changed",
		Workers: 1, BatchSize: 1, PollInterval: time.Second, LeaseDuration: time.Minute,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if err := publisher.publishBatch(context.Background()); err == nil {
		t.Fatal("publishBatch() error = nil, want invalid envelope error")
	}
	if len(repository.failed) != 1 || repository.failed[0] != id {
		t.Fatalf("failed = %v, want invalid event id", repository.failed)
	}
}

func envelopeJSON(envelope event.Envelope) ([]byte, error) {
	return json.Marshal(envelope)
}
