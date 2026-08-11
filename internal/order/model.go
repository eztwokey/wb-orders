package order

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

type Status string

const (
	StatusPending   Status = "PENDING"
	StatusConfirmed Status = "CONFIRMED"
	StatusFailed    Status = "FAILED"
	CurrencyRUB            = "RUB"
)

var (
	ErrNotFound       = errors.New("order not found")
	ErrInvalidRequest = errors.New("invalid order request")
)

type Item struct {
	SKU            string `json:"sku"`
	Quantity       int32  `json:"quantity"`
	UnitPriceMinor int64  `json:"unit_price_minor"`
}

type Order struct {
	ID               uuid.UUID `json:"id"`
	CustomerID       uuid.UUID `json:"customer_id"`
	WarehouseID      uuid.UUID `json:"warehouse_id"`
	Status           Status    `json:"status"`
	Currency         string    `json:"currency"`
	TotalAmountMinor int64     `json:"total_amount_minor"`
	Items            []Item    `json:"items"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type CreateCommand struct {
	CustomerID  uuid.UUID
	WarehouseID uuid.UUID
	Items       []Item
}

func New(command CreateCommand) (Order, error) {
	total, err := validate(command)
	if err != nil {
		return Order{}, err
	}

	now := time.Now().UTC()
	return Order{
		ID:               uuid.New(),
		CustomerID:       command.CustomerID,
		WarehouseID:      command.WarehouseID,
		Status:           StatusPending,
		Currency:         CurrencyRUB,
		TotalAmountMinor: total,
		Items:            append([]Item(nil), command.Items...),
		CreatedAt:        now,
		UpdatedAt:        now,
	}, nil
}

func validate(command CreateCommand) (int64, error) {
	if command.CustomerID == uuid.Nil {
		return 0, fmt.Errorf("%w: customer_id is required", ErrInvalidRequest)
	}
	if command.WarehouseID == uuid.Nil {
		return 0, fmt.Errorf("%w: warehouse_id is required", ErrInvalidRequest)
	}
	if len(command.Items) == 0 {
		return 0, fmt.Errorf("%w: at least one item is required", ErrInvalidRequest)
	}

	var total int64
	seen := make(map[string]struct{}, len(command.Items))
	for _, item := range command.Items {
		if strings.TrimSpace(item.SKU) == "" || utf8.RuneCountInString(item.SKU) > 128 {
			return 0, fmt.Errorf("%w: sku must contain between 1 and 128 characters", ErrInvalidRequest)
		}
		if item.Quantity <= 0 || item.UnitPriceMinor <= 0 {
			return 0, fmt.Errorf("%w: every item needs positive quantity and price", ErrInvalidRequest)
		}
		if _, exists := seen[item.SKU]; exists {
			return 0, fmt.Errorf("%w: duplicate sku %q", ErrInvalidRequest, item.SKU)
		}
		seen[item.SKU] = struct{}{}
		const maxInt64 = int64(^uint64(0) >> 1)
		quantity := int64(item.Quantity)
		if item.UnitPriceMinor > maxInt64/quantity {
			return 0, fmt.Errorf("%w: total amount overflow", ErrInvalidRequest)
		}
		lineTotal := quantity * item.UnitPriceMinor
		if total > maxInt64-lineTotal {
			return 0, fmt.Errorf("%w: total amount overflow", ErrInvalidRequest)
		}
		total += lineTotal
	}
	return total, nil
}

type CreatedPayload struct {
	CustomerID       uuid.UUID `json:"customer_id"`
	WarehouseID      uuid.UUID `json:"warehouse_id"`
	Currency         string    `json:"currency"`
	TotalAmountMinor int64     `json:"total_amount_minor"`
	Items            []Item    `json:"items"`
}

func (p CreatedPayload) Validate() error {
	if p.Currency != CurrencyRUB {
		return fmt.Errorf("%w: unsupported currency %q", ErrInvalidRequest, p.Currency)
	}
	total, err := validate(CreateCommand{CustomerID: p.CustomerID, WarehouseID: p.WarehouseID, Items: p.Items})
	if err != nil {
		return err
	}
	if total != p.TotalAmountMinor {
		return fmt.Errorf("%w: total amount does not match items", ErrInvalidRequest)
	}
	return nil
}

type StatusChangedPayload struct {
	PreviousStatus Status `json:"previous_status"`
	CurrentStatus  Status `json:"current_status"`
	Reason         string `json:"reason,omitempty"`
}
