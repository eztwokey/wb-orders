package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/eztwokey/wb-orders/internal/event"
	"github.com/eztwokey/wb-orders/internal/order"
	platformmetrics "github.com/eztwokey/wb-orders/internal/platform/metrics"
	"github.com/eztwokey/wb-orders/internal/processing"
	"github.com/eztwokey/wb-orders/internal/workerpool"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Repository interface {
	IsProcessed(context.Context, uuid.UUID, string) (bool, error)
	Complete(context.Context, uuid.UUID, string, uuid.UUID, order.Status, string, string) (bool, error)
}

type RecordProducer interface {
	PublishRecord(context.Context, *kgo.Record) error
}

type OrderProcessor interface {
	Process(context.Context, processing.Input) (processing.Result, error)
}

type Config struct {
	ConsumerName string
	RetryTopic   string
	DLQTopic     string
	Workers      int
	BatchSize    int
	MaxRetries   int
	BaseBackoff  time.Duration
	MaxBackoff   time.Duration
	Timeout      time.Duration
}

type Consumer struct {
	client     *kgo.Client
	repository Repository
	producer   RecordProducer
	processor  OrderProcessor
	config     Config
	logger     *slog.Logger
}

const databaseTimeout = 5 * time.Second

func New(client *kgo.Client, repository Repository, producer RecordProducer, processor OrderProcessor, config Config, logger *slog.Logger) *Consumer {
	return &Consumer{client: client, repository: repository, producer: producer, processor: processor, config: config, logger: logger}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		fetches := c.client.PollRecords(ctx, c.config.BatchSize)
		if ctx.Err() != nil {
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fetchErr := range errs {
				c.logger.Error("kafka fetch failed", "topic", fetchErr.Topic, "partition", fetchErr.Partition, "error", fetchErr.Err)
			}
			c.client.AllowRebalance()
			continue
		}

		records := fetches.Records()
		if len(records) == 0 {
			c.client.AllowRebalance()
			continue
		}
		errs := workerpool.Process(ctx, c.config.Workers, records, c.processRecord)
		var batchErr error
		for _, err := range errs {
			if err != nil {
				batchErr = errors.Join(batchErr, err)
				c.logger.Error("kafka record was not handled", "error", err)
			}
		}
		if batchErr != nil {
			c.client.AllowRebalance()
			return fmt.Errorf("kafka batch incomplete; restart from committed offsets: %w", batchErr)
		}
		commitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := c.client.CommitRecords(commitCtx, records...)
		cancel()
		if err != nil {
			c.client.AllowRebalance()
			return fmt.Errorf("commit kafka batch: %w", err)
		}
		c.client.AllowRebalance()
	}
}

func (c *Consumer) processRecord(ctx context.Context, record *kgo.Record) error {
	attempt, err := headerInt(record, "attempt")
	if err != nil {
		return c.dlq(ctx, record, 0, err.Error())
	}
	if attempt > c.config.MaxRetries {
		return c.dlq(ctx, record, attempt, "retry limit already exceeded")
	}
	notBefore, err := headerTime(record, "not_before")
	if err != nil {
		return c.dlq(ctx, record, attempt, err.Error())
	}
	if notBefore.After(time.Now().Add(c.config.MaxBackoff + 5*time.Second)) {
		return c.dlq(ctx, record, attempt, "not_before exceeds configured retry delay")
	}
	if err := waitUntil(ctx, notBefore); err != nil {
		return err
	}

	var envelope event.Envelope
	if err := json.Unmarshal(record.Value, &envelope); err != nil {
		return c.dlq(ctx, record, attempt, "invalid JSON: "+err.Error())
	}
	if err := envelope.Validate(); err != nil || envelope.EventType != event.OrderCreated {
		if err == nil {
			err = fmt.Errorf("unsupported event type %q", envelope.EventType)
		}
		return c.dlq(ctx, record, attempt, err.Error())
	}
	if string(record.Key) != envelope.AggregateID.String() {
		return c.dlq(ctx, record, attempt, "Kafka key does not match aggregate_id")
	}

	log := c.logger.With("request_id", envelope.RequestID, "event_id", envelope.EventID, "order_id", envelope.AggregateID, "topic", record.Topic)
	checkCtx, cancelCheck := context.WithTimeout(ctx, databaseTimeout)
	processed, err := c.repository.IsProcessed(checkCtx, envelope.EventID, c.config.ConsumerName)
	cancelCheck()
	if err != nil {
		return c.retry(ctx, record, attempt, err)
	}
	if processed {
		platformmetrics.KafkaProcessed.WithLabelValues(record.Topic, "duplicate").Inc()
		log.Info("duplicate event skipped")
		return nil
	}

	var payload order.CreatedPayload
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		return c.dlq(ctx, record, attempt, "invalid payload: "+err.Error())
	}
	if err := payload.Validate(); err != nil {
		return c.dlq(ctx, record, attempt, "invalid payload: "+err.Error())
	}
	started := time.Now()
	processCtx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	result, err := c.processor.Process(processCtx, processing.Input{
		OrderID: envelope.AggregateID.String(), WarehouseID: payload.WarehouseID.String(),
		Currency: payload.Currency, TotalAmountMinor: payload.TotalAmountMinor, Items: payload.Items,
	})
	cancel()
	platformmetrics.OrderProcessingDuration.Observe(time.Since(started).Seconds())
	if err != nil {
		if errors.Is(err, context.Canceled) && ctx.Err() != nil {
			return err
		}
		return c.retry(ctx, record, attempt, err)
	}

	completeCtx, cancelComplete := context.WithTimeout(ctx, databaseTimeout)
	applied, err := c.repository.Complete(completeCtx, envelope.EventID, c.config.ConsumerName, envelope.AggregateID, result.Status, result.Reason, envelope.RequestID)
	cancelComplete()
	if err != nil {
		return c.retry(ctx, record, attempt, err)
	}
	if !applied {
		platformmetrics.KafkaProcessed.WithLabelValues(record.Topic, "duplicate").Inc()
		log.Info("event became duplicate or stale during commit")
		return nil
	}
	platformmetrics.KafkaProcessed.WithLabelValues(record.Topic, "success").Inc()
	log.Info("order event processed", "status", result.Status)
	return nil
}

func (c *Consumer) retry(ctx context.Context, record *kgo.Record, currentAttempt int, cause error) error {
	attempt := currentAttempt + 1
	if attempt > c.config.MaxRetries {
		return c.dlq(ctx, record, attempt, cause.Error())
	}
	delay := backoff(attempt, c.config.BaseBackoff, c.config.MaxBackoff)
	err := c.route(ctx, record, c.config.RetryTopic, attempt, cause.Error(), time.Now().Add(delay))
	if err == nil {
		platformmetrics.KafkaProcessed.WithLabelValues(record.Topic, "retry").Inc()
	}
	return err
}

func (c *Consumer) dlq(ctx context.Context, record *kgo.Record, attempt int, reason string) error {
	err := c.route(ctx, record, c.config.DLQTopic, attempt, reason, time.Time{})
	if err == nil {
		platformmetrics.KafkaProcessed.WithLabelValues(record.Topic, "dlq").Inc()
	}
	return err
}

func (c *Consumer) route(ctx context.Context, source *kgo.Record, topic string, attempt int, reason string, notBefore time.Time) error {
	originalTopic := headerString(source, "original_topic")
	if originalTopic == "" {
		originalTopic = source.Topic
	}
	headers := copyHeadersWithout(source.Headers, "attempt", "failure_reason", "not_before", "original_topic")
	headers = append(headers,
		kgo.RecordHeader{Key: "attempt", Value: []byte(strconv.Itoa(attempt))},
		kgo.RecordHeader{Key: "failure_reason", Value: []byte(truncate(reason, 1024))},
		kgo.RecordHeader{Key: "original_topic", Value: []byte(originalTopic)},
	)
	if !notBefore.IsZero() {
		headers = append(headers, kgo.RecordHeader{Key: "not_before", Value: []byte(notBefore.UTC().Format(time.RFC3339Nano))})
	}
	record := &kgo.Record{Topic: topic, Key: append([]byte(nil), source.Key...), Value: append([]byte(nil), source.Value...), Headers: headers}
	publishCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := c.producer.PublishRecord(publishCtx, record); err != nil {
		return fmt.Errorf("route record to %s: %w", topic, err)
	}
	return nil
}

func headerInt(record *kgo.Record, key string) (int, error) {
	for _, header := range record.Headers {
		if header.Key == key {
			value, err := strconv.Atoi(string(header.Value))
			if err != nil || value < 0 {
				return 0, fmt.Errorf("invalid %s header", key)
			}
			return value, nil
		}
	}
	return 0, nil
}

func headerTime(record *kgo.Record, key string) (time.Time, error) {
	for _, header := range record.Headers {
		if header.Key == key {
			value, err := time.Parse(time.RFC3339Nano, string(header.Value))
			if err != nil {
				return time.Time{}, fmt.Errorf("invalid %s header", key)
			}
			return value, nil
		}
	}
	return time.Time{}, nil
}

func headerString(record *kgo.Record, key string) string {
	for _, header := range record.Headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}

func waitUntil(ctx context.Context, target time.Time) error {
	if target.IsZero() || !time.Now().Before(target) {
		return nil
	}
	timer := time.NewTimer(time.Until(target))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func backoff(attempt int, base, maximum time.Duration) time.Duration {
	delay := min(base, maximum)
	for i := 1; i < attempt; i++ {
		if delay >= maximum/2 {
			return maximum
		}
		delay *= 2
	}
	return min(delay, maximum)
}

func copyHeadersWithout(headers []kgo.RecordHeader, excluded ...string) []kgo.RecordHeader {
	set := make(map[string]struct{}, len(excluded))
	for _, key := range excluded {
		set[key] = struct{}{}
	}
	result := make([]kgo.RecordHeader, 0, len(headers))
	for _, header := range headers {
		if _, skip := set[header.Key]; !skip {
			result = append(result, kgo.RecordHeader{Key: header.Key, Value: append([]byte(nil), header.Value...)})
		}
	}
	return result
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}
