package shutdown

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func Context(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
}

func Wait(root context.Context, cancel context.CancelFunc, errCh <-chan error, timeout time.Duration) error {
	remaining := 2
	var result error
	select {
	case result = <-errCh:
		remaining--
	case <-root.Done():
	}
	cancel()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for remaining > 0 {
		select {
		case err := <-errCh:
			remaining--
			if result == nil {
				result = err
			}
		case <-timer.C:
			return fmt.Errorf("application shutdown exceeded %s", timeout)
		}
	}
	if root.Err() != nil {
		return nil
	}
	return result
}
