package order

import (
	"context"
	"testing"

	"github.com/eztwokey/wb-orders/internal/event"
	"github.com/google/uuid"
)

type recordingStore struct {
	order Order
	event event.Envelope
}

func (s *recordingStore) CreateWithEvent(_ context.Context, created Order, envelope event.Envelope) error {
	s.order, s.event = created, envelope
	return nil
}

func (s *recordingStore) Get(_ context.Context, id uuid.UUID) (Order, error) {
	if s.order.ID != id {
		return Order{}, ErrNotFound
	}
	return s.order, nil
}

func TestServiceCreatesOrderAndOutboxEnvelopeTogether(t *testing.T) {
	t.Parallel()
	store := &recordingStore{}
	service := NewService(store)
	created, err := service.Create(context.Background(), CreateCommand{
		CustomerID:  uuid.New(),
		WarehouseID: uuid.New(),
		Items:       []Item{{SKU: "sku-1", Quantity: 2, UnitPriceMinor: 500}},
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if store.order.ID != created.ID {
		t.Fatalf("stored order id = %s, want %s", store.order.ID, created.ID)
	}
	if store.event.AggregateID != created.ID || store.event.EventType != event.OrderCreated {
		t.Fatalf("unexpected event: %+v", store.event)
	}
}
