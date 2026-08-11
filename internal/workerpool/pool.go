package workerpool

import (
	"context"
	"fmt"
	"sync"
)

type Handler[T any] func(context.Context, T) error

// Process handles a finite batch with bounded concurrency and returns one error per item.
func Process[T any](ctx context.Context, workers int, items []T, handler Handler[T]) []error {
	if workers < 1 {
		panic("workerpool: workers must be positive")
	}
	type job struct {
		index int
		item  T
	}

	jobs := make(chan job)
	errorsByIndex := make([]error, len(items))
	var wg sync.WaitGroup
	actualWorkers := min(workers, max(1, len(items)))
	wg.Add(actualWorkers)
	for i := 0; i < actualWorkers; i++ {
		go func() {
			defer wg.Done()
			for job := range jobs {
				if ctx.Err() != nil {
					errorsByIndex[job.index] = fmt.Errorf("worker pool canceled: %w", ctx.Err())
					continue
				}
				errorsByIndex[job.index] = handler(ctx, job.item)
			}
		}()
	}

	for index, item := range items {
		jobs <- job{index: index, item: item}
	}
	close(jobs)
	wg.Wait()
	return errorsByIndex
}
