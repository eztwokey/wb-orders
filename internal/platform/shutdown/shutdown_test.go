package shutdown

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWaitReturnsApplicationErrorAndCancelsPeer(t *testing.T) {
	t.Parallel()
	root := context.Background()
	ctx, cancel := context.WithCancel(root)
	errCh := make(chan error, 2)
	want := errors.New("worker failed")
	errCh <- want
	go func() {
		<-ctx.Done()
		errCh <- nil
	}()

	if err := Wait(root, cancel, errCh, time.Second); !errors.Is(err, want) {
		t.Fatalf("Wait() error = %v, want %v", err, want)
	}
}

func TestWaitBoundsSignalShutdown(t *testing.T) {
	t.Parallel()
	root, stop := context.WithCancel(context.Background())
	stop()
	errCh := make(chan error, 2)
	started := time.Now()
	err := Wait(root, func() {}, errCh, 20*time.Millisecond)
	if err == nil {
		t.Fatal("Wait() error = nil, want shutdown timeout")
	}
	if time.Since(started) > time.Second {
		t.Fatal("Wait() did not honor shutdown timeout")
	}
}
