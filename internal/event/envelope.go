package event

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	OrderCreated       = "OrderCreated"
	OrderStatusChanged = "OrderStatusChanged"
)

type Envelope struct {
	EventID     uuid.UUID       `json:"event_id"`
	EventType   string          `json:"event_type"`
	AggregateID uuid.UUID       `json:"aggregate_id"`
	RequestID   string          `json:"request_id"`
	OccurredAt  time.Time       `json:"occurred_at"`
	Version     int             `json:"version"`
	Payload     json.RawMessage `json:"payload"`
}

func New(eventType string, aggregateID uuid.UUID, requestID string, payload any) (Envelope, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("marshal event payload: %w", err)
	}
	envelope := Envelope{
		EventID:     uuid.New(),
		EventType:   eventType,
		AggregateID: aggregateID,
		RequestID:   requestID,
		OccurredAt:  time.Now().UTC(),
		Version:     1,
		Payload:     raw,
	}
	if err := envelope.Validate(); err != nil {
		return Envelope{}, err
	}
	return envelope, nil
}

func (e Envelope) Validate() error {
	switch {
	case e.EventID == uuid.Nil:
		return fmt.Errorf("event_id is required")
	case e.AggregateID == uuid.Nil:
		return fmt.Errorf("aggregate_id is required")
	case e.EventType == "":
		return fmt.Errorf("event_type is required")
	case strings.TrimSpace(e.RequestID) == "" || len(e.RequestID) > 128:
		return fmt.Errorf("request_id must contain between 1 and 128 characters")
	case e.OccurredAt.IsZero():
		return fmt.Errorf("occurred_at is required")
	case e.Version != 1:
		return fmt.Errorf("unsupported event version: %d", e.Version)
	case len(e.Payload) == 0:
		return fmt.Errorf("payload is required")
	default:
		return nil
	}
}
