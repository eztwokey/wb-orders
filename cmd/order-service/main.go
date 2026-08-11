package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/eztwokey/wb-orders/internal/order"
	"github.com/eztwokey/wb-orders/internal/platform/config"
	"github.com/eztwokey/wb-orders/internal/platform/database"
	"github.com/eztwokey/wb-orders/internal/platform/health"
	"github.com/eztwokey/wb-orders/internal/platform/logging"
	"github.com/eztwokey/wb-orders/internal/platform/shutdown"
)

func main() {
	slog.SetDefault(logging.New("wb-orders-api", config.String("LOG_LEVEL", "info")))
	if err := run(); err != nil {
		slog.Error("order-service stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	root, stop := shutdown.Context(context.Background())
	defer stop()

	cfg, err := config.LoadCommon("wb-orders-api", ":8080")
	if err != nil {
		return err
	}
	logger := logging.New(cfg.ServiceName, cfg.LogLevel)
	pool, err := database.Open(root, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	store := order.NewPostgresStore(pool)
	service := order.NewService(store)
	api := order.NewHTTPHandler(service, logger)
	operational := health.Handler(func(ctx context.Context) error { return pool.Ping(ctx) })
	handler := api.Routes(operational)

	return order.ListenAndServe(root, cfg.HealthAddr, handler, logger, 15*time.Second)
}
