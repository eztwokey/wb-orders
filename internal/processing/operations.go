package processing

import (
	"context"
	"fmt"
)

type Inventory struct{ MaxQuantityPerSKU int32 }

func (Inventory) Name() string { return "inventory" }

func (i Inventory) Execute(ctx context.Context, input Input) (Outcome, error) {
	if err := ctx.Err(); err != nil {
		return Outcome{Operation: i.Name(), Required: true}, err
	}
	limit := i.MaxQuantityPerSKU
	if limit <= 0 {
		limit = 100
	}
	for _, item := range input.Items {
		if item.Quantity > limit {
			return Outcome{Operation: i.Name(), Required: true, Details: fmt.Sprintf("insufficient stock for %s", item.SKU)}, nil
		}
	}
	return Outcome{
		Operation: i.Name(), Required: true, Success: true,
		Details: "reserved at warehouse " + input.WarehouseID,
	}, nil
}

type Delivery struct{}

func (Delivery) Name() string { return "delivery" }

func (d Delivery) Execute(ctx context.Context, input Input) (Outcome, error) {
	if err := ctx.Err(); err != nil {
		return Outcome{Operation: d.Name(), Required: true}, err
	}
	return Outcome{Operation: d.Name(), Required: true, Success: true, Details: "calculated"}, nil
}

type Notification struct{}

func (Notification) Name() string { return "notification" }

func (n Notification) Execute(ctx context.Context, input Input) (Outcome, error) {
	if err := ctx.Err(); err != nil {
		return Outcome{Operation: n.Name(), Required: false}, err
	}
	return Outcome{Operation: n.Name(), Success: true, Details: "prepared"}, nil
}
