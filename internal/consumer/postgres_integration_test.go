//go:build integration

package consumer

import (
	"context"
	"os"
	"testing"

	"github.com/eztwokey/wb-orders/internal/event"
	"github.com/eztwokey/wb-orders/internal/order"
	"github.com/eztwokey/wb-orders/internal/platform/database"
	"github.com/google/uuid"
)

func TestCompleteIsIdempotent(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	pool, err := database.Open(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	orderStore := order.NewPostgresStore(pool)
	created, err := order.New(order.CreateCommand{
		CustomerID: uuid.New(), WarehouseID: uuid.New(),
		Items: []order.Item{{SKU: "idempotency", Quantity: 1, UnitPriceMinor: 100}},
	})
	if err != nil {
		t.Fatal(err)
	}
	createdEvent, err := event.New(event.OrderCreated, created.ID, "integration-test", order.CreatedPayload{
		CustomerID: created.CustomerID, WarehouseID: created.WarehouseID, Currency: created.Currency,
		TotalAmountMinor: created.TotalAmountMinor, Items: created.Items,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := orderStore.CreateWithEvent(ctx, created, createdEvent); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM processed_events WHERE event_id = $1", createdEvent.EventID)
		_, _ = pool.Exec(context.Background(), "DELETE FROM outbox_events WHERE aggregate_id = $1", created.ID)
		_, _ = pool.Exec(context.Background(), "DELETE FROM orders WHERE id = $1", created.ID)
	})

	repository := NewPostgresRepository(pool)
	applied, err := repository.Complete(ctx, createdEvent.EventID, "integration-test", created.ID, order.StatusConfirmed, "", createdEvent.RequestID)
	if err != nil || !applied {
		t.Fatalf("first Complete() = %v, %v", applied, err)
	}
	applied, err = repository.Complete(ctx, createdEvent.EventID, "integration-test", created.ID, order.StatusConfirmed, "", createdEvent.RequestID)
	if err != nil || applied {
		t.Fatalf("second Complete() = %v, %v; want duplicate no-op", applied, err)
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM processed_events WHERE event_id = $1`, createdEvent.EventID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("processed event count = %d, want 1", count)
	}
}
