package order

import (
	"context"
	"fmt"

	"github.com/eztwokey/wb-orders/internal/event"
	"github.com/eztwokey/wb-orders/internal/platform/correlation"
	"github.com/google/uuid"
)

type Store interface {
	CreateWithEvent(context.Context, Order, event.Envelope) error
	Get(context.Context, uuid.UUID) (Order, error)
}

type Service struct{ store Store }

func NewService(store Store) *Service { return &Service{store: store} }

func (s *Service) Create(ctx context.Context, command CreateCommand) (Order, error) {
	created, err := New(command)
	if err != nil {
		return Order{}, err
	}
	requestID := correlation.RequestID(ctx)
	if requestID == "" {
		requestID = uuid.NewString()
	}
	envelope, err := event.New(event.OrderCreated, created.ID, requestID, CreatedPayload{
		CustomerID:       created.CustomerID,
		WarehouseID:      created.WarehouseID,
		Currency:         created.Currency,
		TotalAmountMinor: created.TotalAmountMinor,
		Items:            created.Items,
	})
	if err != nil {
		return Order{}, fmt.Errorf("build order created event: %w", err)
	}
	if err := s.store.CreateWithEvent(ctx, created, envelope); err != nil {
		return Order{}, fmt.Errorf("create order: %w", err)
	}
	return created, nil
}

func (s *Service) Get(ctx context.Context, id uuid.UUID) (Order, error) {
	return s.store.Get(ctx, id)
}
