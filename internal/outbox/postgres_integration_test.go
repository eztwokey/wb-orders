//go:build integration

package outbox

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/eztwokey/wb-orders/internal/event"
	"github.com/eztwokey/wb-orders/internal/platform/database"
	"github.com/google/uuid"
)

func TestConcurrentClaimsDoNotOverlap(t *testing.T) {
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

	aggregateID := uuid.New()
	ids := make([]uuid.UUID, 10)
	for index := range ids {
		envelope, err := event.New(event.OrderCreated, aggregateID, "integration-test", map[string]int{"index": index})
		if err != nil {
			t.Fatal(err)
		}
		ids[index] = envelope.EventID
		raw, _ := json.Marshal(envelope)
		if _, err := pool.Exec(ctx, `INSERT INTO outbox_events (id, aggregate_id, event_type, payload, created_at) VALUES ($1, $2, $3, $4, $5)`, envelope.EventID, aggregateID, envelope.EventType, raw, envelope.OccurredAt); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM outbox_events WHERE aggregate_id = $1", aggregateID)
	})

	repository := NewPostgresRepository(pool)
	var wg sync.WaitGroup
	claimed := make(chan Event, 10)
	for _, owner := range []string{"publisher-a", "publisher-b"} {
		owner := owner
		wg.Add(1)
		go func() {
			defer wg.Done()
			events, err := repository.Claim(ctx, owner, 5, time.Minute)
			if err != nil {
				t.Errorf("Claim(%s) error = %v", owner, err)
				return
			}
			for _, claimedEvent := range events {
				claimed <- claimedEvent
			}
		}()
	}
	wg.Wait()
	close(claimed)

	seen := make(map[uuid.UUID]struct{}, 10)
	for claimedEvent := range claimed {
		if _, duplicate := seen[claimedEvent.ID]; duplicate {
			t.Fatalf("event %s claimed more than once", claimedEvent.ID)
		}
		seen[claimedEvent.ID] = struct{}{}
	}
	if len(seen) != 10 {
		t.Fatalf("claimed %d events, want 10", len(seen))
	}
}
