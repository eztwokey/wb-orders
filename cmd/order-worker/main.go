package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/eztwokey/wb-orders/internal/consumer"
	"github.com/eztwokey/wb-orders/internal/platform/config"
	"github.com/eztwokey/wb-orders/internal/platform/database"
	"github.com/eztwokey/wb-orders/internal/platform/health"
	platformkafka "github.com/eztwokey/wb-orders/internal/platform/kafka"
	"github.com/eztwokey/wb-orders/internal/platform/logging"
	"github.com/eztwokey/wb-orders/internal/platform/shutdown"
	"github.com/eztwokey/wb-orders/internal/processing"
)

func main() {
	slog.SetDefault(logging.New("wb-orders-worker", config.String("LOG_LEVEL", "info")))
	if err := run(); err != nil {
		slog.Error("order-worker stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	root, stop := shutdown.Context(context.Background())
	defer stop()
	cfg, err := config.LoadCommon("wb-orders-worker", ":8082")
	if err != nil {
		return err
	}
	workers, err := config.Int("ORDER_WORKERS", 8)
	if err != nil {
		return err
	}
	batchSize, err := config.Int("KAFKA_BATCH_SIZE", workers*2)
	if err != nil {
		return err
	}
	maxRetries, err := config.Int("KAFKA_MAX_RETRIES", 5)
	if err != nil {
		return err
	}
	processingTimeout, err := config.Duration("ORDER_PROCESSING_TIMEOUT", 30*time.Second)
	if err != nil {
		return err
	}
	baseBackoff, err := config.Duration("KAFKA_RETRY_BASE_BACKOFF", time.Second)
	if err != nil {
		return err
	}
	maxBackoff, err := config.Duration("KAFKA_RETRY_MAX_BACKOFF", time.Minute)
	if err != nil {
		return err
	}

	logger := logging.New(cfg.ServiceName, cfg.LogLevel)
	pool, err := database.Open(root, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	createdTopic := config.String("KAFKA_ORDERS_CREATED_TOPIC", "wb.orders.created")
	retryTopic := config.String("KAFKA_ORDERS_RETRY_TOPIC", "wb.orders.created.retry")
	dlqTopic := config.String("KAFKA_ORDERS_DLQ_TOPIC", "wb.orders.created.dlq")
	rebalanceTimeout := 5 * time.Minute
	worstCaseBatch := time.Duration((batchSize+workers-1)/workers) * (maxBackoff + processingTimeout + 15*time.Second)
	if worstCaseBatch >= rebalanceTimeout {
		return fmt.Errorf("Kafka batch worst-case duration %s must be below rebalance timeout %s", worstCaseBatch, rebalanceTimeout)
	}
	consumerClient, err := platformkafka.NewConsumer(
		cfg.KafkaBrokers,
		cfg.ServiceName,
		config.String("KAFKA_CONSUMER_GROUP", "wb-orders-worker-v1"),
		[]string{createdTopic, retryTopic},
		rebalanceTimeout,
	)
	if err != nil {
		return err
	}
	defer consumerClient.Close()
	producer, err := platformkafka.NewProducer(cfg.KafkaBrokers, cfg.ServiceName+"-retry-producer")
	if err != nil {
		return err
	}
	defer producer.Close()

	repository := consumer.NewPostgresRepository(pool)
	processor := processing.New(processing.Inventory{MaxQuantityPerSKU: 100}, processing.Delivery{}, processing.Notification{})
	worker := consumer.New(consumerClient, repository, producer, processor, consumer.Config{
		ConsumerName: config.String("CONSUMER_NAME", "wb-orders-worker-v1"),
		RetryTopic:   retryTopic,
		DLQTopic:     dlqTopic,
		Workers:      workers,
		BatchSize:    batchSize,
		MaxRetries:   maxRetries,
		BaseBackoff:  baseBackoff,
		MaxBackoff:   maxBackoff,
		Timeout:      processingTimeout,
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
	go func() { errCh <- worker.Run(ctx) }()
	go func() { errCh <- health.Serve(ctx, cfg.HealthAddr, ready, logger, 10*time.Second) }()

	return shutdown.Wait(root, cancel, errCh, 15*time.Second)
}
