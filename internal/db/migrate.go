package db

import (
	"context"
	"embed"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var embedded embed.FS

// Migrate runs a goose command: "up" applies all pending migrations, "down"
// rolls back exactly one version, "status" changes nothing. It returns the
// current schema version afterwards.
func Migrate(ctx context.Context, pool *pgxpool.Pool, command string) (int64, error) {
	switch command {
	case "up", "down", "status":
	default:
		return 0, fmt.Errorf("migrate: unknown command %q (want up|down|status)", command)
	}

	migrations, err := fs.Sub(embedded, "migrations")
	if err != nil {
		return 0, fmt.Errorf("migrate: sub fs: %w", err)
	}
	sqlDB := stdlib.OpenDBFromPool(pool)
	defer sqlDB.Close()

	p, err := goose.NewProvider(goose.DialectPostgres, sqlDB, migrations)
	if err != nil {
		return 0, fmt.Errorf("migrate: new provider: %w", err)
	}
	switch command {
	case "up":
		_, err = p.Up(ctx)
	case "down":
		_, err = p.Down(ctx)
	}
	if err != nil {
		return 0, fmt.Errorf("migrate %s: %w", command, err)
	}
	v, err := p.GetDBVersion(ctx)
	if err != nil {
		return 0, fmt.Errorf("migrate %s: get version: %w", command, err)
	}
	return v, nil
}
