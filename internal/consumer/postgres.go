package consumer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/eztwokey/wb-orders/internal/event"
	"github.com/eztwokey/wb-orders/internal/order"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepository struct{ pool *pgxpool.Pool }

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

func (r *PostgresRepository) IsProcessed(ctx context.Context, eventID uuid.UUID, consumerName string) (bool, error) {
	var exists bool
	err := r.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM processed_events WHERE event_id = $1 AND consumer_name = $2
		)`, eventID, consumerName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check processed event: %w", err)
	}
	return exists, nil
}

// Complete atomically records idempotency, changes order state and emits OrderStatusChanged.
func (r *PostgresRepository) Complete(
	ctx context.Context,
	eventID uuid.UUID,
	consumerName string,
	orderID uuid.UUID,
	status order.Status,
	reason string,
	requestID string,
) (bool, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("begin completion transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, `
		INSERT INTO processed_events (event_id, consumer_name)
		VALUES ($1, $2)
		ON CONFLICT DO NOTHING`, eventID, consumerName)
	if err != nil {
		return false, fmt.Errorf("record processed event: %w", err)
	}
	if tag.RowsAffected() == 0 {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit duplicate event transaction: %w", err)
		}
		return false, nil
	}

	var previous order.Status
	if err := tx.QueryRow(ctx, `SELECT status FROM orders WHERE id = $1 FOR UPDATE`, orderID).Scan(&previous); err != nil {
		return false, fmt.Errorf("lock order: %w", err)
	}
	if previous != order.StatusPending {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit stale event transaction: %w", err)
		}
		return false, nil
	}

	if _, err := tx.Exec(ctx, `UPDATE orders SET status = $2, updated_at = now() WHERE id = $1`, orderID, status); err != nil {
		return false, fmt.Errorf("update order status: %w", err)
	}
	statusEvent, err := event.New(event.OrderStatusChanged, orderID, requestID, order.StatusChangedPayload{
		PreviousStatus: previous,
		CurrentStatus:  status,
		Reason:         reason,
	})
	if err != nil {
		return false, err
	}
	payload, err := json.Marshal(statusEvent)
	if err != nil {
		return false, fmt.Errorf("marshal status event: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox_events (id, aggregate_id, event_type, payload, created_at)
		VALUES ($1, $2, $3, $4, $5)`, statusEvent.EventID, orderID, statusEvent.EventType, payload, statusEvent.OccurredAt); err != nil {
		return false, fmt.Errorf("insert status outbox event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit completion transaction: %w", err)
	}
	return true, nil
}
