package main

import (
	"context"
	"log"
	"os"

	_ "github.com/jackc/pgx/v5/pgconn"
	"github.com/MrTeeett/pgx-v5-pool-examples/pgx_demo"
)

func main() {
	ctx := context.Background()

	// 1) Берем строку подключения. Важно: pool_* параметры (pool_max_conns, pool_min_conns,
	// pool_min_idle_conns и т.д.) — это ПАРАМЕТРЫ ПУЛА, а не соединения.
	// ParseConfig распознает их и кладет ВНЕ ConnConfig, чтобы они НЕ попадали в DSN при установке коннекта.
	dsn := os.Getenv("PGURL")
	if dsn == "" {
		// Фолбэк (для локальных тестов). Вы можете сразу прописать pool_* прямо в URL-квери.
		// Эти параметры считает ParseConfig, а внутри ConnConfig их уже не будет.
		dsn = "postgres://myadmin:masterkey@localhost:5432/app?sslmode=disable&pool_max_conns=10&pool_min_conns=2&pool_min_idle_conns=1"
	}

	// 1) СНАЧАЛА — bootstrap через ЧИСТЫЙ ConnConfig (без pool_*)
	if err := pgx_demo.BootstrapEnsureSchema(ctx, dsn); err != nil {
		log.Fatalf("bootstrapEnsureSchema: %v", err)
	}

	// ТЕПЕРЬ поднимаем пул и спокойно готовим prepared в AfterConnect
	pool, err := pgx_demo.BuildPool(ctx, dsn)
	if err != nil {
		log.Fatalf("buildPool failed: %v", err)
	}
	defer pool.Close()

	// 2) Явный health-check: Pool.Ping берет коннект из пула, вызывает Conn.Ping и возвращает его обратно.
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("База недоступна: %v", err)
	}
	log.Println("Ping OK — база отвечает")

	// 3) Подготовим базу (DDL). Можно держать это в миграциях (goose/tern/etc.),
	// тут — компактно для демонстрации.
	if err := pgx_demo.EnsureSchema(ctx, pool); err != nil {
		log.Fatalf("ensureSchema: %v", err)
	}

	// 4) Регистрация/логин пользователя с транзакцией.
	email := "alice@example.com"
	name := "Alice"
	// middle_name может быть NULL — покажем работу pgtype.Text{Valid:false}
	var middleName *string // nil => NULL
	userID, err := pgx_demo.UpsertUserAndLogLogin(ctx, pool, email, name, middleName)
	if err != nil {
		log.Fatalf("upsertUserAndLogLogin: %v", err)
	}
	log.Printf("Пользователь id=%d готов\n", userID)

	// 5) Реалистичный кейс: «гарантируем» аккаунт и читаем баланс (Numeric через pgtype).
	if err := pgx_demo.EnsureAccount(ctx, pool, userID); err != nil {
		log.Fatalf("ensureAccount: %v", err)
	}
	bal, err := pgx_demo.GetBalance(ctx, pool, userID)
	if err != nil {
		log.Fatalf("getBalance: %v", err)
	}
	log.Printf("Начальный баланс пользователя %d: %s\n", userID, bal.Int) // bal.String() уже человекочитаемый

	// 6) Пример acquire/release вручную — показываем, что можно брать соединение прямо из пула,
	// а не всегда через pool.Exec/Query*. Внутри Acquire пул проверит «живость» —
	// там используется внутренняя логика вида ShouldPing: если пора/нужно — будет Ping,
	// мертвые соединения закрываются и берется следующее.
	if err := pgx_demo.SampleAcquireRelease(ctx, pool); err != nil {
		log.Fatalf("sampleAcquireRelease: %v", err)
	}

	// 7) Работа с NULL и pgtype.* при сканировании.
	if err := pgx_demo.DemoScanWithPgtype(ctx, pool, email); err != nil {
		log.Fatalf("demoScanWithPgtype: %v", err)
	}

	// 8) Достанем FieldDescriptions() для метаданных запроса — имена и типы колонок результата.
	if err := pgx_demo.ShowQueryMetadata(ctx, pool); err != nil {
		log.Fatalf("showQueryMetadata: %v", err)
	}

	log.Println("Демонстрация завершена успешно")
}
