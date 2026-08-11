package outbox

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Event struct {
	ID          uuid.UUID
	AggregateID uuid.UUID
	EventType   string
	Payload     json.RawMessage
	Attempts    int
	CreatedAt   time.Time
}

type Topics struct {
	OrderCreated       string
	OrderStatusChanged string
}

func (t Topics) For(eventType string) (string, bool) {
	switch eventType {
	case "OrderCreated":
		return t.OrderCreated, true
	case "OrderStatusChanged":
		return t.OrderStatusChanged, true
	default:
		return "", false
	}
}
