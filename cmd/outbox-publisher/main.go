package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/eztwokey/wb-orders/internal/outbox"
	"github.com/eztwokey/wb-orders/internal/platform/config"
	"github.com/eztwokey/wb-orders/internal/platform/database"
	"github.com/eztwokey/wb-orders/internal/platform/health"
	platformkafka "github.com/eztwokey/wb-orders/internal/platform/kafka"
	"github.com/eztwokey/wb-orders/internal/platform/logging"
	"github.com/eztwokey/wb-orders/internal/platform/shutdown"
	"github.com/google/uuid"
)

func main() {
	slog.SetDefault(logging.New("wb-orders-outbox-publisher", config.String("LOG_LEVEL", "info")))
	if err := run(); err != nil {
		slog.Error("outbox-publisher stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	root, stop := shutdown.Context(context.Background())
	defer stop()
	cfg, err := config.LoadCommon("wb-orders-outbox-publisher", ":8081")
	if err != nil {
		return err
	}
	workers, err := config.Int("OUTBOX_WORKERS", 4)
	if err != nil {
		return err
	}
	batchSize, err := config.Int("OUTBOX_BATCH_SIZE", 50)
	if err != nil {
		return err
	}
	poll, err := config.Duration("OUTBOX_POLL_INTERVAL", time.Second)
	if err != nil {
		return err
	}
	lease, err := config.Duration("OUTBOX_LEASE_DURATION", 4*time.Minute)
	if err != nil {
		return err
	}
	minimumLease := time.Duration((batchSize+workers-1)/workers)*(outbox.PublishTimeout+outbox.DatabaseTimeout) + 5*time.Second
	if lease < minimumLease {
		return fmt.Errorf("OUTBOX_LEASE_DURATION must be at least %s for the configured batch and worker count", minimumLease)
	}

	logger := logging.New(cfg.ServiceName, cfg.LogLevel)
	pool, err := database.Open(root, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	producer, err := platformkafka.NewProducer(cfg.KafkaBrokers, cfg.ServiceName)
	if err != nil {
		return err
	}
	defer producer.Close()

	hostname, _ := os.Hostname()
	owner := fmt.Sprintf("%s-%s", hostname, uuid.NewString())
	repository := outbox.NewPostgresRepository(pool)
	publisher := outbox.NewPublisher(repository, producer, outbox.PublisherConfig{
		Owner:                   owner,
		OrderCreatedTopic:       config.String("KAFKA_ORDERS_CREATED_TOPIC", "wb.orders.created"),
		OrderStatusChangedTopic: config.String("KAFKA_STATUS_CHANGED_TOPIC", "wb.orders.status-changed"),
		Workers:                 workers,
		BatchSize:               batchSize,
		PollInterval:            poll,
		LeaseDuration:           lease,
	}, logger)

	ctx, cancel := context.WithCancel(root)
	defer cancel()
	ready := health.Handler(func(ctx context.Context) error {
		if err := pool.Ping(ctx); err != nil {
			return err
		}
		return producer.Ping(ctx)
	})
	errCh := make(chan error, 2)
	go func() { errCh <- publisher.Run(ctx) }()
	go func() { errCh <- health.Serve(ctx, cfg.HealthAddr, ready, logger, 10*time.Second) }()

	return shutdown.Wait(root, cancel, errCh, 15*time.Second)
}
