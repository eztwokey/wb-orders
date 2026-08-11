package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepository struct{ pool *pgxpool.Pool }

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

func (r *PostgresRepository) Claim(ctx context.Context, owner string, limit int, lease time.Duration) ([]Event, error) {
	rows, err := r.pool.Query(ctx, `
		WITH candidates AS (
			SELECT id
				FROM outbox_events
				WHERE status IN ('PENDING', 'PROCESSING')
			  AND next_attempt_at <= now()
			  AND (locked_until IS NULL OR locked_until < now())
			ORDER BY created_at
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		UPDATE outbox_events AS o
		SET status = 'PROCESSING',
			locked_by = $2,
			locked_until = now() + ($3 * interval '1 millisecond'),
			attempts = attempts + 1
		FROM candidates
		WHERE o.id = candidates.id
		RETURNING o.id, o.aggregate_id, o.event_type, o.payload, o.attempts, o.created_at`,
		limit, owner, lease.Milliseconds(),
	)
	if err != nil {
		return nil, fmt.Errorf("claim outbox events: %w", err)
	}
	defer rows.Close()

	events := make([]Event, 0, limit)
	for rows.Next() {
		var claimed Event
		if err := rows.Scan(&claimed.ID, &claimed.AggregateID, &claimed.EventType, &claimed.Payload, &claimed.Attempts, &claimed.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan claimed outbox event: %w", err)
		}
		events = append(events, claimed)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimed outbox events: %w", err)
	}
	return events, nil
}

func (r *PostgresRepository) MarkPublished(ctx context.Context, id uuid.UUID, owner string) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE outbox_events
		SET status = 'PUBLISHED', published_at = now(), locked_by = NULL, locked_until = NULL, last_error = NULL
		WHERE id = $1 AND locked_by = $2 AND published_at IS NULL`, id, owner)
	if err != nil {
		return fmt.Errorf("mark outbox event published: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("mark outbox event published: lease lost")
	}
	return nil
}

func (r *PostgresRepository) Release(ctx context.Context, id uuid.UUID, owner, message string, nextAttempt time.Time) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE outbox_events
		SET status = 'PENDING', locked_by = NULL, locked_until = NULL,
			last_error = left($3, 2000), next_attempt_at = $4
		WHERE id = $1 AND locked_by = $2 AND published_at IS NULL`, id, owner, message, nextAttempt)
	if err != nil {
		return fmt.Errorf("release outbox event: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("release outbox event: lease lost")
	}
	return nil
}

func (r *PostgresRepository) MarkFailed(ctx context.Context, id uuid.UUID, owner, message string) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE outbox_events
		SET status = 'FAILED', locked_by = NULL, locked_until = NULL, last_error = left($3, 2000)
		WHERE id = $1 AND locked_by = $2 AND published_at IS NULL`, id, owner, message)
	if err != nil {
		return fmt.Errorf("mark outbox event failed: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("mark outbox event failed: lease lost")
	}
	return nil
}

func (r *PostgresRepository) PendingCount(ctx context.Context) (int64, error) {
	var count int64
	err := r.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE status IN ('PENDING', 'PROCESSING')`).Scan(&count)
	return count, err
}
