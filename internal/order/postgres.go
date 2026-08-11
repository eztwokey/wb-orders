package order

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/eztwokey/wb-orders/internal/event"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresStore struct{ pool *pgxpool.Pool }

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore { return &PostgresStore{pool: pool} }

func (s *PostgresStore) CreateWithEvent(ctx context.Context, created Order, envelope event.Envelope) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, err = tx.Exec(ctx, `
		INSERT INTO orders (id, customer_id, warehouse_id, status, currency, total_amount_minor, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		created.ID, created.CustomerID, created.WarehouseID, created.Status, created.Currency,
		created.TotalAmountMinor, created.CreatedAt, created.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert order: %w", err)
	}

	for _, item := range created.Items {
		_, err = tx.Exec(ctx, `
			INSERT INTO order_items (order_id, sku, quantity, unit_price_minor)
			VALUES ($1, $2, $3, $4)`, created.ID, item.SKU, item.Quantity, item.UnitPriceMinor)
		if err != nil {
			return fmt.Errorf("insert order item %q: %w", item.SKU, err)
		}
	}

	payload, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal outbox envelope: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (id, aggregate_id, event_type, payload, created_at)
		VALUES ($1, $2, $3, $4, $5)`,
		envelope.EventID, envelope.AggregateID, envelope.EventType, payload, envelope.OccurredAt,
	)
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

func (s *PostgresStore) Get(ctx context.Context, id uuid.UUID) (Order, error) {
	var found Order
	err := s.pool.QueryRow(ctx, `
		SELECT id, customer_id, warehouse_id, status, currency, total_amount_minor, created_at, updated_at
		FROM orders
		WHERE id = $1`, id,
	).Scan(&found.ID, &found.CustomerID, &found.WarehouseID, &found.Status, &found.Currency,
		&found.TotalAmountMinor, &found.CreatedAt, &found.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Order{}, ErrNotFound
	}
	if err != nil {
		return Order{}, fmt.Errorf("select order: %w", err)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT sku, quantity, unit_price_minor
		FROM order_items
		WHERE order_id = $1
		ORDER BY sku`, id)
	if err != nil {
		return Order{}, fmt.Errorf("select order items: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var item Item
		if err := rows.Scan(&item.SKU, &item.Quantity, &item.UnitPriceMinor); err != nil {
			return Order{}, fmt.Errorf("scan order item: %w", err)
		}
		found.Items = append(found.Items, item)
	}
	if err := rows.Err(); err != nil {
		return Order{}, fmt.Errorf("iterate order items: %w", err)
	}
	return found, nil
}
