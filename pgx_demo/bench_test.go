// bench_test.go
// Небольшие бенчмарки:
// 1) acquire/release — базовая издержка выдачи соединения,
// 2) minimal prepared select — сравнение «без prepare» и «с prepare».
// Запускайте: go test -bench=. -benchmem

package pgx_demo

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func benchPool(b *testing.B) *pgxpool.Pool {
	dsn := os.Getenv("PGURL")
	if dsn == "" {
		dsn = "postgres://myadmin:masterkey@localhost:5432/app?sslmode=disable&pool_max_conns=10&pool_min_conns=2&pool_min_idle_conns=1"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := BuildPool(ctx, dsn)
	if err != nil {
		b.Fatalf("buildPool: %v", err)
	}
	b.Cleanup(func() { pool.Close() })

	// Убедимся, что таблицы есть и есть хотя бы один пользователь.
	if err := EnsureSchema(ctx, pool); err != nil {
		b.Fatalf("ensureSchema: %v", err)
	}
	if _, err := UpsertUserAndLogLogin(ctx, pool, "bench@example.com", "Bench", nil); err != nil {
		b.Fatalf("seed user: %v", err)
	}
	return pool
}

func BenchmarkAcquireAndRelease(b *testing.B) {
	pool := benchPool(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c, err := pool.Acquire(ctx)
		if err != nil {
			b.Fatal(err)
		}
		c.Release()
	}
}

func BenchmarkMinimalPreparedSelectBaseline(b *testing.B) {
	// Базовый «без prepare» путь: каждый раз обычный текст запроса.
	pool := benchPool(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var id int64
		err := pool.QueryRow(ctx,
			`SELECT id FROM app_users WHERE email = $1`,
			"bench@example.com",
		).Scan(&id)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMinimalPreparedSelect(b *testing.B) {
	pool := benchPool(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var id int64
		err := pool.QueryRow(ctx, psGetUserIdByEmail, "bench@example.com").Scan(&id)
		if err != nil {
			b.Fatal(err)
		}
	}
}
