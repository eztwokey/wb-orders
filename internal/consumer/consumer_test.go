package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/eztwokey/wb-orders/internal/event"
	"github.com/eztwokey/wb-orders/internal/order"
	"github.com/eztwokey/wb-orders/internal/processing"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeConsumerRepository struct {
	processed bool
	applied   bool
	complete  int
}

func (r *fakeConsumerRepository) IsProcessed(context.Context, uuid.UUID, string) (bool, error) {
	return r.processed, nil
}
func (r *fakeConsumerRepository) Complete(context.Context, uuid.UUID, string, uuid.UUID, order.Status, string, string) (bool, error) {
	r.complete++
	return r.applied, nil
}

type fakeOrderProcessor struct {
	result processing.Result
	err    error
	calls  int
}

func (p *fakeOrderProcessor) Process(context.Context, processing.Input) (processing.Result, error) {
	p.calls++
	return p.result, p.err
}

type recordingProducer struct{ records []*kgo.Record }

func (p *recordingProducer) PublishRecord(_ context.Context, record *kgo.Record) error {
	p.records = append(p.records, record)
	return nil
}

func newTestRecord(t *testing.T) *kgo.Record {
	t.Helper()
	orderID := uuid.New()
	payload := order.CreatedPayload{
		CustomerID: uuid.New(), WarehouseID: uuid.New(), Currency: order.CurrencyRUB,
		TotalAmountMinor: 100, Items: []order.Item{{SKU: "sku", Quantity: 1, UnitPriceMinor: 100}},
	}
	envelope, err := event.New(event.OrderCreated, orderID, "consumer-test", payload)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: "wb.orders.created", Key: []byte(orderID.String()), Value: raw}
}

func newTestConsumer(repository Repository, producer RecordProducer, processor OrderProcessor) *Consumer {
	return New(nil, repository, producer, processor, Config{
		ConsumerName: "test", RetryTopic: "retry", DLQTopic: "dlq", Workers: 1, BatchSize: 2,
		MaxRetries: 3, BaseBackoff: time.Millisecond, MaxBackoff: time.Second, Timeout: time.Second,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestProcessRecordSkipsKnownDuplicate(t *testing.T) {
	t.Parallel()
	repository := &fakeConsumerRepository{processed: true}
	processor := &fakeOrderProcessor{}
	producer := &recordingProducer{}
	consumer := newTestConsumer(repository, producer, processor)

	if err := consumer.processRecord(context.Background(), newTestRecord(t)); err != nil {
		t.Fatalf("processRecord() error = %v", err)
	}
	if processor.calls != 0 || repository.complete != 0 {
		t.Fatalf("duplicate performed work: processor=%d complete=%d", processor.calls, repository.complete)
	}
}

func TestProcessRecordCompletesSuccessfulOrder(t *testing.T) {
	t.Parallel()
	repository := &fakeConsumerRepository{applied: true}
	processor := &fakeOrderProcessor{result: processing.Result{Status: order.StatusConfirmed}}
	consumer := newTestConsumer(repository, &recordingProducer{}, processor)

	if err := consumer.processRecord(context.Background(), newTestRecord(t)); err != nil {
		t.Fatalf("processRecord() error = %v", err)
	}
	if processor.calls != 1 || repository.complete != 1 {
		t.Fatalf("work counts: processor=%d complete=%d, want 1 each", processor.calls, repository.complete)
	}
}

func TestProcessRecordRoutesTechnicalFailureToRetry(t *testing.T) {
	t.Parallel()
	repository := &fakeConsumerRepository{}
	processor := &fakeOrderProcessor{err: errors.New("inventory timeout")}
	producer := &recordingProducer{}
	consumer := newTestConsumer(repository, producer, processor)

	if err := consumer.processRecord(context.Background(), newTestRecord(t)); err != nil {
		t.Fatalf("processRecord() error = %v", err)
	}
	if len(producer.records) != 1 || producer.records[0].Topic != "retry" {
		t.Fatalf("retry records = %+v", producer.records)
	}
	if attempt, err := headerInt(producer.records[0], "attempt"); err != nil || attempt != 1 {
		t.Fatalf("retry attempt = %d, want 1", attempt)
	}
}

func TestProcessRecordRoutesMalformedRetryHeaderToDLQ(t *testing.T) {
	t.Parallel()
	producer := &recordingProducer{}
	consumer := newTestConsumer(&fakeConsumerRepository{}, producer, &fakeOrderProcessor{})
	record := newTestRecord(t)
	record.Headers = append(record.Headers, kgo.RecordHeader{Key: "attempt", Value: []byte("broken")})

	if err := consumer.processRecord(context.Background(), record); err != nil {
		t.Fatalf("processRecord() error = %v", err)
	}
	if len(producer.records) != 1 || producer.records[0].Topic != "dlq" {
		t.Fatalf("DLQ records = %+v", producer.records)
	}
}

func TestProcessRecordRoutesMalformedNotBeforeHeaderToDLQ(t *testing.T) {
	t.Parallel()
	producer := &recordingProducer{}
	consumer := newTestConsumer(&fakeConsumerRepository{}, producer, &fakeOrderProcessor{})
	record := newTestRecord(t)
	record.Headers = append(record.Headers, kgo.RecordHeader{Key: "not_before", Value: []byte("tomorrow")})

	if err := consumer.processRecord(context.Background(), record); err != nil {
		t.Fatalf("processRecord() error = %v", err)
	}
	if len(producer.records) != 1 || producer.records[0].Topic != "dlq" {
		t.Fatalf("DLQ records = %+v", producer.records)
	}
}

func TestBackoffIsExponentiallyCapped(t *testing.T) {
	t.Parallel()
	base, maximum := time.Second, 5*time.Second
	tests := []struct {
		attempt int
		want    time.Duration
	}{{1, time.Second}, {2, 2 * time.Second}, {3, 4 * time.Second}, {4, 5 * time.Second}, {10, 5 * time.Second}}
	for _, test := range tests {
		if got := backoff(test.attempt, base, maximum); got != test.want {
			t.Errorf("backoff(%d) = %s, want %s", test.attempt, got, test.want)
		}
	}
}

func TestWaitUntilHonorsCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitUntil(ctx, time.Now().Add(time.Hour)); err == nil {
		t.Fatal("waitUntil() error = nil, want cancellation")
	}
}

func TestCopyHeadersWithoutDoesNotMutateSource(t *testing.T) {
	t.Parallel()
	source := []kgo.RecordHeader{{Key: "attempt", Value: []byte("1")}, {Key: "trace", Value: []byte("abc")}}
	result := copyHeadersWithout(source, "attempt")
	if len(result) != 1 || result[0].Key != "trace" {
		t.Fatalf("copyHeadersWithout() = %+v", result)
	}
	result[0].Value[0] = 'z'
	if string(source[1].Value) != "abc" {
		t.Fatal("source header was mutated")
	}
}
