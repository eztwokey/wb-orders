package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/eztwokey/wb-orders/internal/event"
	platformmetrics "github.com/eztwokey/wb-orders/internal/platform/metrics"
	"github.com/eztwokey/wb-orders/internal/workerpool"
	"github.com/google/uuid"
)

type Repository interface {
	Claim(context.Context, string, int, time.Duration) ([]Event, error)
	MarkPublished(context.Context, uuid.UUID, string) error
	Release(context.Context, uuid.UUID, string, string, time.Time) error
	MarkFailed(context.Context, uuid.UUID, string, string) error
	PendingCount(context.Context) (int64, error)
}

type Producer interface {
	Publish(context.Context, string, string, []byte, event.Envelope) error
}

const (
	PublishTimeout  = 10 * time.Second
	DatabaseTimeout = 5 * time.Second
)

type Publisher struct {
	repository Repository
	producer   Producer
	topics     Topics
	owner      string
	workers    int
	batchSize  int
	poll       time.Duration
	lease      time.Duration
	logger     *slog.Logger
}

type PublisherConfig struct {
	Owner, OrderCreatedTopic, OrderStatusChangedTopic string
	Workers, BatchSize                                int
	PollInterval, LeaseDuration                       time.Duration
}

func NewPublisher(repository Repository, producer Producer, config PublisherConfig, logger *slog.Logger) *Publisher {
	return &Publisher{
		repository: repository, producer: producer, owner: config.Owner,
		topics:  Topics{OrderCreated: config.OrderCreatedTopic, OrderStatusChanged: config.OrderStatusChangedTopic},
		workers: config.Workers, batchSize: config.BatchSize, poll: config.PollInterval,
		lease: config.LeaseDuration, logger: logger,
	}
}

func (p *Publisher) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.poll)
	defer ticker.Stop()

	for {
		if err := p.publishBatch(ctx); err != nil && ctx.Err() == nil {
			p.logger.Error("outbox batch failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (p *Publisher) publishBatch(ctx context.Context) error {
	claimCtx, cancelClaim := context.WithTimeout(ctx, DatabaseTimeout)
	claimed, err := p.repository.Claim(claimCtx, p.owner, p.batchSize, p.lease)
	cancelClaim()
	if err != nil {
		return err
	}
	countCtx, cancelCount := context.WithTimeout(ctx, DatabaseTimeout)
	count, countErr := p.repository.PendingCount(countCtx)
	cancelCount()
	if countErr == nil {
		platformmetrics.OutboxPending.Set(float64(count))
	}
	if len(claimed) == 0 {
		return nil
	}

	errs := workerpool.Process(ctx, p.workers, claimed, p.publishOne)
	for _, publishErr := range errs {
		if publishErr != nil {
			return publishErr
		}
	}
	return nil
}

func (p *Publisher) publishOne(ctx context.Context, claimed Event) error {
	topic, ok := p.topics.For(claimed.EventType)
	if !ok {
		err := fmt.Errorf("unsupported outbox event type %q", claimed.EventType)
		return p.failPermanently(ctx, claimed, "unsupported", err)
	}

	var envelope event.Envelope
	if err := json.Unmarshal(claimed.Payload, &envelope); err != nil {
		return p.failPermanently(ctx, claimed, "invalid", fmt.Errorf("decode outbox event %s: %w", claimed.ID, err))
	}
	if err := envelope.Validate(); err != nil {
		return p.failPermanently(ctx, claimed, "invalid", fmt.Errorf("validate outbox event %s: %w", claimed.ID, err))
	}
	if envelope.EventID != claimed.ID || envelope.AggregateID != claimed.AggregateID || envelope.EventType != claimed.EventType {
		return p.failPermanently(ctx, claimed, "invalid", fmt.Errorf("outbox columns do not match envelope %s", claimed.ID))
	}

	publishCtx, cancel := context.WithTimeout(ctx, PublishTimeout)
	defer cancel()
	if err := p.producer.Publish(publishCtx, topic, claimed.AggregateID.String(), claimed.Payload, envelope); err != nil {
		delay := exponentialBackoff(claimed.Attempts, time.Second, time.Minute)
		releaseCtx, cancelRelease := context.WithTimeout(ctx, DatabaseTimeout)
		releaseErr := p.repository.Release(releaseCtx, claimed.ID, p.owner, err.Error(), time.Now().Add(delay))
		cancelRelease()
		if releaseErr != nil {
			return fmt.Errorf("publish and release outbox event: %w", errors.Join(err, releaseErr))
		}
		platformmetrics.OutboxPublished.WithLabelValues("failed").Inc()
		return err
	}
	markCtx, cancelMark := context.WithTimeout(ctx, DatabaseTimeout)
	err := p.repository.MarkPublished(markCtx, claimed.ID, p.owner)
	cancelMark()
	if err != nil {
		return err
	}
	platformmetrics.OutboxPublished.WithLabelValues("success").Inc()
	p.logger.Info("outbox event published", "request_id", envelope.RequestID, "event_id", claimed.ID, "order_id", claimed.AggregateID, "topic", topic)
	return nil
}

func (p *Publisher) failPermanently(ctx context.Context, claimed Event, result string, cause error) error {
	failCtx, cancelFail := context.WithTimeout(ctx, DatabaseTimeout)
	err := p.repository.MarkFailed(failCtx, claimed.ID, p.owner, cause.Error())
	cancelFail()
	if err != nil {
		return fmt.Errorf("reject outbox event: %w", errors.Join(cause, err))
	}
	platformmetrics.OutboxPublished.WithLabelValues(result).Inc()
	return cause
}

func exponentialBackoff(attempt int, base, maximum time.Duration) time.Duration {
	exponent := min(max(attempt-1, 0), 10)
	delay := time.Duration(float64(base) * math.Pow(2, float64(exponent)))
	return min(delay, maximum)
}
