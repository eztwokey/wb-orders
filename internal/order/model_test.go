package order

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestNewCalculatesTotalInMinorUnits(t *testing.T) {
	t.Parallel()
	created, err := New(CreateCommand{
		CustomerID:  uuid.New(),
		WarehouseID: uuid.New(),
		Items: []Item{
			{SKU: "book", Quantity: 2, UnitPriceMinor: 1299},
			{SKU: "pen", Quantity: 3, UnitPriceMinor: 150},
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if want := int64(3048); created.TotalAmountMinor != want {
		t.Fatalf("TotalAmountMinor = %d, want %d", created.TotalAmountMinor, want)
	}
	if created.Status != StatusPending {
		t.Fatalf("Status = %s, want %s", created.Status, StatusPending)
	}
}

func TestNewRejectsDuplicateSKU(t *testing.T) {
	t.Parallel()
	_, err := New(CreateCommand{
		CustomerID:  uuid.New(),
		WarehouseID: uuid.New(),
		Items:       []Item{{SKU: "same", Quantity: 1, UnitPriceMinor: 1}, {SKU: "same", Quantity: 2, UnitPriceMinor: 2}},
	})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("New() error = %v, want ErrInvalidRequest", err)
	}
}

func TestNewRejectsInvalidSKULength(t *testing.T) {
	t.Parallel()
	for _, sku := range []string{"   ", strings.Repeat("x", 129)} {
		_, err := New(CreateCommand{CustomerID: uuid.New(), WarehouseID: uuid.New(), Items: []Item{{SKU: sku, Quantity: 1, UnitPriceMinor: 100}}})
		if !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("New() error = %v, want ErrInvalidRequest for SKU length", err)
		}
	}
}

func TestNewRejectsAmountOverflow(t *testing.T) {
	t.Parallel()
	_, err := New(CreateCommand{
		CustomerID:  uuid.New(),
		WarehouseID: uuid.New(),
		Items:       []Item{{SKU: "expensive", Quantity: 2, UnitPriceMinor: int64(^uint64(0) >> 1)}},
	})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("New() error = %v, want overflow validation error", err)
	}
}

func TestNewUsesRUBCurrency(t *testing.T) {
	t.Parallel()
	created, err := New(CreateCommand{
		CustomerID: uuid.New(), WarehouseID: uuid.New(),
		Items: []Item{{SKU: "rub-price", Quantity: 1, UnitPriceMinor: 12345}},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if created.Currency != CurrencyRUB {
		t.Fatalf("Currency = %q, want %q", created.Currency, CurrencyRUB)
	}
}

func TestNewRequiresWarehouse(t *testing.T) {
	t.Parallel()
	_, err := New(CreateCommand{
		CustomerID: uuid.New(),
		Items:      []Item{{SKU: "warehouse-required", Quantity: 1, UnitPriceMinor: 100}},
	})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("New() error = %v, want ErrInvalidRequest", err)
	}
}

func TestCreatedPayloadRejectsUnsupportedCurrency(t *testing.T) {
	t.Parallel()
	payload := CreatedPayload{
		CustomerID: uuid.New(), WarehouseID: uuid.New(), Currency: "USD",
		TotalAmountMinor: 100,
		Items:            []Item{{SKU: "currency-check", Quantity: 1, UnitPriceMinor: 100}},
	}
	if err := payload.Validate(); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Validate() error = %v, want ErrInvalidRequest", err)
	}
}
