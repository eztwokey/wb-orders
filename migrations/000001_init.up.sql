CREATE TYPE order_status AS ENUM ('PENDING', 'CONFIRMED', 'FAILED');
CREATE TYPE outbox_status AS ENUM ('PENDING', 'PROCESSING', 'PUBLISHED', 'FAILED');

CREATE TABLE orders (
    id UUID PRIMARY KEY,
    customer_id UUID NOT NULL,
    warehouse_id UUID NOT NULL,
    status order_status NOT NULL,
    currency CHAR(3) NOT NULL CHECK (currency = 'RUB'),
    total_amount_minor BIGINT NOT NULL CHECK (total_amount_minor > 0),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (updated_at >= created_at)
);

CREATE INDEX idx_orders_customer_created_at
    ON orders (customer_id, created_at DESC);
CREATE INDEX idx_orders_status_created_at
    ON orders (status, created_at);
CREATE INDEX idx_orders_warehouse_status_created_at
    ON orders (warehouse_id, status, created_at);

CREATE TABLE order_items (
    order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    sku TEXT NOT NULL CHECK (length(sku) BETWEEN 1 AND 128),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price_minor BIGINT NOT NULL CHECK (unit_price_minor > 0),
    PRIMARY KEY (order_id, sku)
);

CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type <> ''),
    payload JSONB NOT NULL,
    status outbox_status NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by TEXT,
    locked_until TIMESTAMPTZ,
    last_error TEXT,
    CHECK ((status = 'PUBLISHED') = (published_at IS NOT NULL)),
    CHECK ((locked_by IS NULL) = (locked_until IS NULL))
);

CREATE INDEX idx_outbox_available
    ON outbox_events (next_attempt_at, created_at)
    WHERE status IN ('PENDING', 'PROCESSING');
CREATE INDEX idx_outbox_expired_leases
    ON outbox_events (locked_until)
    WHERE status = 'PROCESSING';
CREATE INDEX idx_outbox_aggregate
    ON outbox_events (aggregate_id, created_at);

CREATE TABLE processed_events (
    event_id UUID NOT NULL,
    consumer_name TEXT NOT NULL CHECK (consumer_name <> ''),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_name, event_id)
);

CREATE INDEX idx_processed_events_processed_at
    ON processed_events (processed_at);
