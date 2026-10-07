// Command chat-analyze is the Telegram userbot entry point.
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/a1ekseev/chat-analyze/internal/app"
	"github.com/a1ekseev/chat-analyze/internal/config"
	"github.com/a1ekseev/chat-analyze/internal/db"
	"github.com/a1ekseev/chat-analyze/internal/httpserver"
	"github.com/a1ekseev/chat-analyze/internal/logx"
	"github.com/a1ekseev/chat-analyze/internal/telegram"
)

// Set via -ldflags "-X main.version=... -X main.commit=...".
var (
	version = "dev"
	commit  = "unknown"
)

const (
	usage        = "usage: chat-analyze run|login|migrate|healthcheck|version"
	migrateUsage = "usage: chat-analyze migrate [up|down|status]"

	defaultHTTPAddr = ":8080"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// run dispatches subcommands and returns the process exit code: 0 on
// success, 1 on failure, 2 on usage errors. The service (run) logs to stdout;
// the one-shot commands log to stderr and keep stdout for user output.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, usage)
		return 2
	}

	switch args[0] {
	case "version":
		fmt.Fprintln(stdout, version+" "+commit)
		return 0
	case "healthcheck":
		addr := os.Getenv("HTTP_ADDR")
		if addr == "" {
			addr = defaultHTTPAddr
		}
		return httpserver.HealthcheckCmd(ctx, addr)
	case "run":
		return runService(ctx, stdout, stderr)
	case "login":
		return runLogin(ctx, stdout, stderr)
	case "migrate":
		command := "up"
		if len(args) > 1 {
			command = args[1]
		}
		switch command {
		case "up", "down", "status":
		default:
			fmt.Fprintln(stderr, migrateUsage)
			return 2
		}
		return runMigrate(ctx, command, stdout, stderr)
	default:
		fmt.Fprintln(stderr, usage)
		return 2
	}
}

// loadConfig loads the config and builds the logger writing to w. On error
// it logs to stderr (no values, only variable names) and returns ok=false.
func loadConfig(ctx context.Context, w, stderr io.Writer) (config.Config, *slog.Logger, bool) {
	cfg, err := config.Load()
	if err != nil {
		fail(ctx, logx.New(stderr, slog.LevelInfo, "json"), "config invalid", err)
		return config.Config{}, nil, false
	}
	level, err := logx.ParseLevel(cfg.LogLevel)
	if err != nil {
		fail(ctx, logx.New(stderr, slog.LevelInfo, cfg.LogFormat), "config invalid", err)
		return config.Config{}, nil, false
	}
	return cfg, logx.New(w, level, cfg.LogFormat), true
}

func fail(ctx context.Context, logger *slog.Logger, msg string, err error) {
	logger.ErrorContext(ctx, msg, logx.Component("app"), logx.Err(err)) //nolint:sloglint // msg is one of a few static strings
}

func runService(ctx context.Context, stdout, stderr io.Writer) int {
	cfg, logger, ok := loadConfig(ctx, stdout, stderr)
	if !ok {
		return 1
	}
	logger.InfoContext(ctx, "service starting", logx.Component("app"),
		slog.String("version", version), slog.String("commit", commit),
		slog.Int("workers", cfg.Workers), slog.Bool("mark_read", cfg.MarkRead),
		slog.String("log_level", cfg.LogLevel))
	if err := app.Run(ctx, cfg, logger); err != nil {
		fail(ctx, logger, "service failed", err)
		return 1
	}
	return 0
}

func runMigrate(ctx context.Context, command string, stdout, stderr io.Writer) int {
	// migrate needs only DATABASE_URL, not the Telegram credentials.
	logger := logx.New(stderr, slog.LevelInfo, "json")
	dsn, err := config.LoadDatabaseFrom(nil)
	if err != nil {
		fail(ctx, logger, "config invalid", err)
		return 1
	}
	pool, err := db.NewPool(ctx, dsn.Reveal())
	if err != nil {
		fail(ctx, logger, "migrate failed", err)
		return 1
	}
	defer pool.Close()
	v, err := db.Migrate(ctx, pool, command)
	if err != nil {
		fail(ctx, logger, "migrate failed", err)
		return 1
	}
	fmt.Fprintf(stdout, "db version %d\n", v)
	return 0
}

func runLogin(ctx context.Context, stdout, stderr io.Writer) int {
	cfg, logger, ok := loadConfig(ctx, stderr, stderr)
	if !ok {
		return 1
	}
	if err := cfg.ValidateLogin(); err != nil {
		fail(ctx, logger, "config invalid", err)
		return 1
	}
	gotdLevel, err := logx.ParseLevel(cfg.TGLogLevel)
	if err != nil {
		fail(ctx, logger, "config invalid", err)
		return 1
	}
	pool, err := db.NewPool(ctx, cfg.DatabaseURL.Reveal())
	if err != nil {
		fail(ctx, logger, "login failed", err)
		return 1
	}
	defer pool.Close()
	if _, err := db.Migrate(ctx, pool, "up"); err != nil {
		fail(ctx, logger, "login failed", err)
		return 1
	}
	err = telegram.Login(ctx, telegram.ClientConfig{
		APIID:        cfg.APIID,
		APIHash:      cfg.APIHash.Reveal(),
		Pool:         pool,
		SessionName:  cfg.SessionName,
		Logger:       logger,
		GotdLogLevel: gotdLevel,
	}, cfg.Phone.Reveal(), os.Stdin, stdout)
	if err != nil {
		fail(ctx, logger, "login failed", err)
		return 1
	}
	return 0
}
