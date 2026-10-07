// Package db owns the Postgres connection pool and the goose migrations.
package db

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PoolOption tweaks the pool configuration before the pool is created.
type PoolOption func(*pgxpool.Config)

// WithSearchPath sets the search_path runtime parameter on every connection.
func WithSearchPath(schema string) PoolOption {
	return func(c *pgxpool.Config) {
		c.ConnConfig.RuntimeParams["search_path"] = schema
	}
}

// WithMaxConnsAtLeast raises the pool size to at least n (never lowers it)
// unless the DSN sets pool_max_conns explicitly, which always wins.
func WithMaxConnsAtLeast(n int) PoolOption {
	return func(c *pgxpool.Config) {
		if strings.Contains(c.ConnString(), "pool_max_conns") || n <= 0 {
			return
		}
		if n > math.MaxInt32 {
			n = math.MaxInt32
		}
		c.MaxConns = max(c.MaxConns, int32(n)) //nolint:gosec // n is clamped to [1, MaxInt32] above
	}
}

// NewPool creates a pgx pool and pings it before returning.
// Errors never include the DSN (it carries the password).
func NewPool(ctx context.Context, dsn string, opts ...PoolOption) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		// pgx's parse error may echo parts of the DSN; keep only the operation.
		return nil, errors.New("parse database config: invalid DSN")
	}
	for _, o := range opts {
		o(cfg)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}
