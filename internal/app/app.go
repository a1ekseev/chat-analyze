// Package app assembles the service and runs its components (spec §3).
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/a1ekseev/chat-analyze/internal/config"
	"github.com/a1ekseev/chat-analyze/internal/db"
	"github.com/a1ekseev/chat-analyze/internal/httpserver"
	"github.com/a1ekseev/chat-analyze/internal/logx"
	"github.com/a1ekseev/chat-analyze/internal/message"
	"github.com/a1ekseev/chat-analyze/internal/queue"
	"github.com/a1ekseev/chat-analyze/internal/telegram"
	"github.com/a1ekseev/chat-analyze/internal/worker"
)

const (
	readHeaderTimeout = 10 * time.Second
	// poolHeadroom is added to WORKERS for the minimum pool size (F12).
	poolHeadroom = 4
)

// Run connects to the database, applies migrations and runs the Telegram
// client, the queue notifier, the workers, maintenance and the HTTP server
// until ctx is cancelled or one of them fails.
func Run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	log := logger.With(logx.Component("app"))

	// Every worker holds a connection while it works; the queue notifier
	// holds one for good; the backfill worker, maintenance, Telegram state
	// and the health check need the rest.
	pool, err := db.NewPool(ctx, cfg.DatabaseURL.Reveal(), db.WithMaxConnsAtLeast(cfg.Workers+poolHeadroom))
	if err != nil {
		return fmt.Errorf("connect database: %w", err)
	}
	closePool := true
	defer func() {
		if closePool {
			pool.Close()
		}
	}()

	started := time.Now()
	version, err := db.Migrate(ctx, pool, "up")
	if err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	log.InfoContext(ctx, "migrations applied", slog.Int64("db_version", version), logx.Duration(time.Since(started)))

	gotdLevel, err := logx.ParseLevel(cfg.TGLogLevel)
	if err != nil {
		return fmt.Errorf("tg log level: %w", err)
	}

	q := queue.New(pool, cfg.JobsMaxAttempts)
	notifier := queue.NewNotifier(pool, logger)
	repo := message.NewRepository(pool)
	peerCache := telegram.NewPeerCache(pool)

	// The listener needs the client's API and the client needs the
	// listener's OnChannelTooLong; gotd calls it only after Run started.
	var listener *telegram.Listener
	client := telegram.NewClient(telegram.ClientConfig{ //nolint:contextcheck // gotd callbacks carry no ctx
		APIID:            cfg.APIID,
		APIHash:          cfg.APIHash.Reveal(),
		Pool:             pool,
		SessionName:      cfg.SessionName,
		Logger:           logger,
		GotdLogLevel:     gotdLevel,
		RPS:              cfg.RPS,
		Peers:            peerCache,
		OnChannelTooLong: func(channelID int64) { listener.OnChannelTooLong(channelID) }, //nolint:contextcheck // listener uses its Start ctx
	})
	listener = telegram.NewListener(q, repo, peerCache, client.API(), logger)
	reader := telegram.NewReader(client.API(), telegram.NewReadMarks(pool), logger)
	wcfg := worker.Config{MarkRead: cfg.MarkRead, Lease: cfg.JobsLease, PollInterval: cfg.JobsPollInterval}

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           httpserver.NewHealth(pool, client.Ready, logger),
		ReadHeaderTimeout: readHeaderTimeout,
	}

	retention := queue.Retention{Done: cfg.JobsRetention, Dead: cfg.JobsDeadRetention}
	s := stages{
		// OnChannelTooLong enqueues on the Background ctx.
		Start: listener.Start,
		Background: []func(context.Context) error{
			notifier.Run,
			func(ctx context.Context) error { return RunMaintenance(ctx, q, retention, logger) },
		},
		Telegram: func(ctx context.Context) error { return client.Run(ctx, listener.Register) },
		Serve: func() error {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return fmt.Errorf("http server: %w", err)
			}
			return nil
		},
		Stop: srv.Shutdown,
	}
	// Read marks are coalesced in the reader and sent every
	// ReadFlushInterval; whatever is pending at shutdown is sent before
	// Telegram stops (or dropped when that takes too long).
	s.Background = append(s.Background, func(ctx context.Context) error {
		return reader.Run(ctx, telegram.ReadFlushInterval)
	})
	s.Flush = func(ctx context.Context) { _ = reader.Flush(ctx) }
	for range cfg.Workers {
		w := worker.New(q, repo, reader, wcfg, logger)
		s.Background = append(s.Background, func(ctx context.Context) error { return w.Run(ctx, notifier.C()) })
	}
	// One backfill_channel worker (ChannelTooLong gaps); it stops with the
	// other workers, before Telegram.
	s.Background = append(s.Background, worker.NewBackfill(q, listener, wcfg, logger).Run)

	err = runStages(ctx, s, cfg.ShutdownTimeout, log)
	if errors.Is(err, ErrShutdownTimeout) {
		// Components still hold connections: closing the pool would block.
		closePool = false
	}
	return err
}
