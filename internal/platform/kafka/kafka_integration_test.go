//go:build integration

package kafka

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/eztwokey/wb-orders/internal/event"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestProducerPublishesKeyEnvelopeAndCorrelationHeaders(t *testing.T) {
	brokersValue := os.Getenv("TEST_KAFKA_BROKERS")
	if brokersValue == "" {
		t.Skip("TEST_KAFKA_BROKERS is not set")
	}
	brokers := strings.Split(brokersValue, ",")
	producer, err := NewProducer(brokers, "kafka-integration-producer")
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	pingDeadline := time.Now().Add(90 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err = producer.Ping(ctx)
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(pingDeadline) {
			t.Fatalf("Kafka did not become ready: %v", err)
		}
		time.Sleep(time.Second)
	}

	orderID := uuid.New()
	envelope, err := event.New(event.OrderCreated, orderID, "kafka-integration-request", map[string]string{"source": "integration"})
	if err != nil {
		t.Fatal(err)
	}
	value, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	topic := "wb-orders.integration." + uuid.NewString()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	err = producer.Publish(ctx, topic, orderID.String(), value, envelope)
	cancel()
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	pollCtx, pollCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer pollCancel()
	for {
		fetches := consumer.PollRecords(pollCtx, 1)
		if pollCtx.Err() != nil {
			t.Fatalf("poll Kafka record: %v", pollCtx.Err())
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			continue
		}
		records := fetches.Records()
		if len(records) == 0 {
			continue
		}
		record := records[0]
		if string(record.Key) != orderID.String() {
			t.Fatalf("record key = %q, want order id", record.Key)
		}
		if headerValue(record.Headers, "event_id") != envelope.EventID.String() {
			t.Fatalf("event_id header was not propagated")
		}
		if headerValue(record.Headers, "request_id") != envelope.RequestID {
			t.Fatalf("request_id header was not propagated")
		}
		return
	}
}

func headerValue(headers []kgo.RecordHeader, key string) string {
	for _, header := range headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}
