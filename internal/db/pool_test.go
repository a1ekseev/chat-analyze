package db

import (
	"runtime"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func parse(t *testing.T, dsn string) *pgxpool.Config {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	return cfg
}

func TestWithMaxConnsAtLeast_RaisesDefault(t *testing.T) {
	cfg := parse(t, "postgres://u@localhost/db")
	WithMaxConnsAtLeast(68)(cfg)
	require.EqualValues(t, 68, cfg.MaxConns)
}

func TestWithMaxConnsAtLeast_NeverLowers(t *testing.T) {
	cfg := parse(t, "postgres://u@localhost/db")
	WithMaxConnsAtLeast(1)(cfg)
	require.EqualValues(t, max(4, runtime.NumCPU()), cfg.MaxConns)
}

func TestWithMaxConnsAtLeast_ExplicitDSNWins(t *testing.T) {
	for _, dsn := range []string{
		"postgres://u@localhost/db?pool_max_conns=3",
		"host=localhost user=u dbname=db pool_max_conns=3",
	} {
		cfg := parse(t, dsn)
		WithMaxConnsAtLeast(68)(cfg)
		require.EqualValues(t, 3, cfg.MaxConns, dsn)
	}
}
