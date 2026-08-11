package processing

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/eztwokey/wb-orders/internal/order"
)

type Input struct {
	OrderID          string
	WarehouseID      string
	Currency         string
	TotalAmountMinor int64
	Items            []order.Item
}

type Outcome struct {
	Operation string
	Success   bool
	Required  bool
	Details   string
}

type Operation interface {
	Name() string
	Execute(context.Context, Input) (Outcome, error)
}

type Result struct {
	Status   order.Status
	Reason   string
	Outcomes []Outcome
}

type Processor struct{ operations []Operation }

func New(operations ...Operation) *Processor {
	return &Processor{operations: append([]Operation(nil), operations...)}
}

func (p *Processor) Process(ctx context.Context, input Input) (Result, error) {
	if len(p.operations) == 0 {
		return Result{}, fmt.Errorf("processing pipeline has no operations")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type operationResult struct {
		outcome Outcome
		err     error
	}
	results := make(chan operationResult, len(p.operations))
	for _, operation := range p.operations {
		operation := operation
		go func() {
			outcome, err := operation.Execute(ctx, input)
			if outcome.Operation == "" {
				outcome.Operation = operation.Name()
			}
			results <- operationResult{outcome: outcome, err: err}
		}()
	}

	var joined error
	outcomes := make([]Outcome, 0, len(p.operations))
	for range p.operations {
		result := <-results
		if result.err != nil {
			joined = errors.Join(joined, fmt.Errorf("%s: %w", result.outcome.Operation, result.err))
			cancel()
		}
		outcomes = append(outcomes, result.outcome)
	}
	if joined != nil {
		return Result{}, joined
	}

	sort.Slice(outcomes, func(i, j int) bool { return outcomes[i].Operation < outcomes[j].Operation })
	var failures []string
	for _, outcome := range outcomes {
		if outcome.Required && !outcome.Success {
			failures = append(failures, outcome.Operation+": "+outcome.Details)
		}
	}
	if len(failures) > 0 {
		return Result{Status: order.StatusFailed, Reason: strings.Join(failures, "; "), Outcomes: outcomes}, nil
	}
	return Result{Status: order.StatusConfirmed, Outcomes: outcomes}, nil
}
