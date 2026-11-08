package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/a1ekseev/chat-analyze/internal/logx"
)

// ErrShutdownTimeout is returned when components do not stop within
// SHUTDOWN_TIMEOUT after the shutdown started.
var ErrShutdownTimeout = errors.New("shutdown timed out")

// stages describes the running components in shutdown order.
type stages struct {
	// Start (optional) is called with the Background ctx before anything runs.
	Start func(ctx context.Context)
	// Background (workers, queue notifier, maintenance) stops first: their
	// ctx is cancelled on shutdown; workers finish the in-flight job.
	Background []func(ctx context.Context) error
	// Flush (optional) runs after Background returned and before Telegram
	// is cancelled (pending read marks), bounded by half the timeout.
	Flush func(ctx context.Context)
	// Telegram is cancelled only after all Background functions returned, so
	// an in-flight job can still mark messages read.
	Telegram func(ctx context.Context) error
	// Serve runs the HTTP server until Stop is called, after Telegram stopped.
	Serve func() error
	Stop  func(ctx context.Context) error
}

// runStages runs s until ctx is cancelled or any component fails (which
// starts the shutdown too), then stops the stages in order. It returns the
// first component error, or ErrShutdownTimeout when the stop takes longer
// than timeout.
func runStages(ctx context.Context, s stages, timeout time.Duration, log *slog.Logger) error {
	root, cancelRoot := context.WithCancel(ctx)
	defer cancelRoot()

	var (
		mu       sync.Mutex
		firstErr error
	)
	fail := func(err error) {
		if err == nil {
			return
		}
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
		cancelRoot()
	}

	if s.Start != nil {
		s.Start(root)
	}
	var bg sync.WaitGroup
	for _, f := range s.Background {
		bg.Go(func() { fail(f(root)) })
	}
	tgCtx, cancelTG := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelTG()
	tgDone := make(chan struct{})
	go func() {
		defer close(tgDone)
		fail(s.Telegram(tgCtx))
	}()
	httpDone := make(chan struct{})
	go func() {
		defer close(httpDone)
		fail(s.Serve())
	}()

	<-root.Done()
	reason := "signal"
	if ctx.Err() == nil {
		reason = "error"
	}
	start := time.Now()
	log.InfoContext(ctx, "shutdown started", slog.String("reason", reason))

	drained := make(chan error, 1)
	go func() {
		bg.Wait()
		if s.Flush != nil {
			fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout/2)
			s.Flush(fctx)
			cancel()
		}
		cancelTG()
		<-tgDone
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		defer cancel()
		stopErr := s.Stop(sctx)
		<-httpDone
		drained <- stopErr
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	var stopErr error
	select {
	case stopErr = <-drained:
	case <-timer.C:
		log.ErrorContext(ctx, "shutdown timed out", logx.Duration(time.Since(start)))
		return ErrShutdownTimeout
	}
	log.InfoContext(ctx, "shutdown complete", slog.String("reason", reason), logx.Duration(time.Since(start)))

	mu.Lock()
	defer mu.Unlock()
	if firstErr != nil {
		return firstErr
	}
	if stopErr != nil {
		return fmt.Errorf("http shutdown: %w", stopErr)
	}
	return nil
}
