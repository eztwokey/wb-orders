package event

import (
	"testing"

	"github.com/google/uuid"
)

func TestNewRequiresRequestID(t *testing.T) {
	t.Parallel()
	if _, err := New(OrderCreated, uuid.New(), "", map[string]string{"value": "test"}); err == nil {
		t.Fatal("New() error = nil, want missing request_id error")
	}
}

func TestNewBuildsValidEnvelope(t *testing.T) {
	t.Parallel()
	envelope, err := New(OrderCreated, uuid.New(), "request-123", map[string]string{"value": "test"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := envelope.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}
