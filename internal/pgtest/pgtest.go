// Package pgtest provides an isolated, migrated Postgres schema per test.
package pgtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/a1ekseev/chat-analyze/internal/db"
)

// New creates schema t_<random> in the database named by TEST_DATABASE_URL,
// returns a migrated pool whose search_path points at it, and drops the schema
// on cleanup. A missing TEST_DATABASE_URL fails the test (never skips).
func New(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn, schema := newSchema(t)
	ctx := context.Background()
	pool, err := db.NewPool(ctx, dsn, db.WithSearchPath(schema))
	if err != nil {
		t.Fatalf("connect test pool: %v", err)
	}
	t.Cleanup(pool.Close) // runs before the schema drop (LIFO)

	if _, err := db.Migrate(ctx, pool, "up"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

// DSN creates an empty (not migrated) schema like New and returns a DSN whose
// search_path points at it, for code that opens its own pool (app.Run).
func DSN(t *testing.T) string {
	t.Helper()
	dsn, schema := newSchema(t)
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal("parse TEST_DATABASE_URL")
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String()
}

func newSchema(t *testing.T) (dsn, schema string) {
	t.Helper()
	dsn = os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("TEST_DATABASE_URL is required for integration tests")
	}
	ctx := context.Background()

	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("random schema name: %v", err)
	}
	schema = "t_" + hex.EncodeToString(b[:])

	admin, err := db.NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("connect admin pool: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close()
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := admin.Exec(cctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("drop schema: %v", err)
		}
		admin.Close()
	})
	return dsn, schema
}
