package processing

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/eztwokey/wb-orders/internal/order"
)

type delayedOperation struct {
	name    string
	delay   time.Duration
	outcome Outcome
	err     error
}

func (o delayedOperation) Name() string { return o.name }
func (o delayedOperation) Execute(ctx context.Context, _ Input) (Outcome, error) {
	select {
	case <-ctx.Done():
		return Outcome{Operation: o.name, Required: true}, ctx.Err()
	case <-time.After(o.delay):
		o.outcome.Operation = o.name
		return o.outcome, o.err
	}
}

func TestProcessorRunsOperationsConcurrently(t *testing.T) {
	t.Parallel()
	processor := New(
		delayedOperation{name: "a", delay: 50 * time.Millisecond, outcome: Outcome{Required: true, Success: true}},
		delayedOperation{name: "b", delay: 50 * time.Millisecond, outcome: Outcome{Required: true, Success: true}},
		delayedOperation{name: "c", delay: 50 * time.Millisecond, outcome: Outcome{Success: true}},
	)
	started := time.Now()
	result, err := processor.Process(context.Background(), Input{})
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if elapsed := time.Since(started); elapsed >= 120*time.Millisecond {
		t.Fatalf("operations appear sequential, elapsed = %s", elapsed)
	}
	if result.Status != order.StatusConfirmed {
		t.Fatalf("status = %s, want CONFIRMED", result.Status)
	}
}

func TestProcessorFailsOrderOnRequiredBusinessFailure(t *testing.T) {
	t.Parallel()
	processor := New(delayedOperation{
		name: "inventory", outcome: Outcome{Required: true, Success: false, Details: "out of stock"},
	})
	result, err := processor.Process(context.Background(), Input{})
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if result.Status != order.StatusFailed {
		t.Fatalf("status = %s, want FAILED", result.Status)
	}
}

func TestProcessorReturnsTechnicalError(t *testing.T) {
	t.Parallel()
	want := errors.New("dependency unavailable")
	processor := New(delayedOperation{name: "inventory", outcome: Outcome{Required: true}, err: want})
	_, err := processor.Process(context.Background(), Input{})
	if !errors.Is(err, want) {
		t.Fatalf("Process() error = %v, want wrapped dependency error", err)
	}
}
