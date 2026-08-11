package workerpool

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestProcessBoundsConcurrency(t *testing.T) {
	t.Parallel()
	items := make([]int, 24)
	var active atomic.Int32
	var maximum atomic.Int32
	errs := Process(context.Background(), 3, items, func(context.Context, int) error {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			previous := maximum.Load()
			if current <= previous || maximum.CompareAndSwap(previous, current) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		return nil
	})
	for _, err := range errs {
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}
	}
	if got := maximum.Load(); got > 3 {
		t.Fatalf("maximum concurrency = %d, want <= 3", got)
	}
}

func TestProcessReportsCanceledItems(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	errs := Process(ctx, 2, []int{1, 2}, func(context.Context, int) error { return nil })
	for index, err := range errs {
		if err == nil {
			t.Fatalf("error[%d] = nil, want cancellation", index)
		}
	}
}
