package testutils

import (
	"context"
	"io"
	"log/slog"
	"net/url"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aditya1808/distributed-rate-limiter/persistence/pgstore"
)

// PostgresURL returns POSTGRES_URL or skips the test.
func PostgresURL(tb testing.TB) string {
	tb.Helper()
	u := os.Getenv("POSTGRES_URL")
	if u == "" {
		tb.Skip("POSTGRES_URL not set; skipping Postgres-backed test")
	}
	return u
}

var schemaUnsafe = regexp.MustCompile(`[^a-z0-9_]+`)

// PostgresSchemaURL isolates a test in its own schema, named after its key
// prefix so apps sharing a prefix (instances of one deployment) also share
// rules. The schema is created on first use and dropped when that test ends.
func PostgresSchemaURL(tb testing.TB, prefix string) string {
	tb.Helper()
	base := PostgresURL(tb)
	schema := schemaUnsafe.ReplaceAllString(strings.ToLower(prefix), "_")
	if len(schema) > 63 {
		schema = schema[len(schema)-63:]
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, base)
	if err != nil {
		tb.Fatalf("postgres not reachable at POSTGRES_URL: %v", err)
	}
	defer conn.Close(ctx)
	var exists bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)`, schema).Scan(&exists); err != nil {
		tb.Fatalf("check schema: %v", err)
	}
	if !exists {
		if _, err := conn.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
			tb.Fatalf("create schema: %v", err)
		}
		tb.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if c, err := pgx.Connect(ctx, base); err == nil {
				_, _ = c.Exec(ctx, `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
				_ = c.Close(ctx)
			}
		})
	}

	u, err := url.Parse(base)
	if err != nil {
		tb.Fatalf("parse POSTGRES_URL: %v", err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String()
}

// PostgresPool returns a migrated pool in a fresh schema.
func PostgresPool(tb testing.TB) *pgxpool.Pool {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, PostgresSchemaURL(tb, "test-"+tb.Name()+"-"+RandomSuffix()))
	if err != nil {
		tb.Fatalf("connect postgres: %v", err)
	}
	tb.Cleanup(pool.Close)
	if err := pgstore.Migrate(ctx, pool, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		tb.Fatalf("migrate: %v", err)
	}
	return pool
}
