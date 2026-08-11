//go:build integration

package order

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/eztwokey/wb-orders/internal/event"
	"github.com/eztwokey/wb-orders/internal/platform/database"
	"github.com/google/uuid"
)

func TestCreateWithEventRollsBackOrderWhenOutboxInsertFails(t *testing.T) {
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

	created, err := New(CreateCommand{
		CustomerID:  uuid.New(),
		WarehouseID: uuid.New(),
		Items:       []Item{{SKU: "rollback-test", Quantity: 1, UnitPriceMinor: 100}},
	})
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := event.New(event.OrderCreated, created.ID, "integration-test", CreatedPayload{
		CustomerID: created.CustomerID, WarehouseID: created.WarehouseID, Currency: created.Currency,
		TotalAmountMinor: created.TotalAmountMinor, Items: created.Items,
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(envelope)
	if _, err := pool.Exec(ctx, `
		INSERT INTO outbox_events (id, aggregate_id, event_type, payload, created_at)
		VALUES ($1, $2, $3, $4, $5)`, envelope.EventID, uuid.New(), envelope.EventType, raw, envelope.OccurredAt); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM outbox_events WHERE id = $1", envelope.EventID)
		_, _ = pool.Exec(context.Background(), "DELETE FROM orders WHERE id = $1", created.ID)
	})

	store := NewPostgresStore(pool)
	if err := store.CreateWithEvent(ctx, created, envelope); err == nil {
		t.Fatal("CreateWithEvent() error = nil, want duplicate outbox id failure")
	}
	var exists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM orders WHERE id = $1)`, created.ID).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("order insert was not rolled back")
	}
}
