package kafka

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/eztwokey/wb-orders/internal/event"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Producer struct{ client *kgo.Client }

func NewProducer(brokers []string, clientID string) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(clientID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}
	return &Producer{client: client}, nil
}

func (p *Producer) Publish(ctx context.Context, topic, key string, value []byte, envelope event.Envelope) error {
	record := &kgo.Record{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
		Headers: []kgo.RecordHeader{
			{Key: "event_id", Value: []byte(envelope.EventID.String())},
			{Key: "event_type", Value: []byte(envelope.EventType)},
			{Key: "event_version", Value: []byte(strconv.Itoa(envelope.Version))},
			{Key: "request_id", Value: []byte(envelope.RequestID)},
		},
	}
	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("produce kafka record: %w", err)
	}
	return nil
}

func (p *Producer) PublishRecord(ctx context.Context, record *kgo.Record) error {
	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("produce kafka record: %w", err)
	}
	return nil
}

func (p *Producer) Ping(ctx context.Context) error { return p.client.Ping(ctx) }

func (p *Producer) Close() { p.client.Close() }
