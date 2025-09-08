package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/MrTeeett/pgx-v5-pool-examples/pgx_demo"
	"github.com/jackc/pgx/v5/pgtype"
)

func main() {
	// Базовый контекст приложения. Для критичных операций ниже будем брать контексты с таймаутом,
	// чтобы не зависать бесконечно при сетевых сбоях.
	rootCtx := context.Background()

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
	// Bootstrap выполняем с таймаутом, чтобы быстро обнаружить проблемы подключения/прав доступа.
	if err := func() error {
		ctx, cancel := context.WithTimeout(rootCtx, 5*time.Second)
		defer cancel()
		return pgx_demo.BootstrapEnsureSchema(ctx, dsn)
	}(); err != nil {
		log.Fatalf("bootstrapEnsureSchema: %v", err)
	}

	// ТЕПЕРЬ поднимаем пул и спокойно готовим prepared в AfterConnect
	pool, err := pgx_demo.BuildPool(rootCtx, dsn)
	if err != nil {
		log.Fatalf("buildPool failed: %v", err)
	}
	defer pool.Close()

	// 2) Явный health-check: Pool.Ping берет коннект из пула, вызывает Conn.Ping и возвращает его обратно.
	// Выполняем с коротким таймаутом — если БД недоступна, быстро узнаем.
	if err := func() error {
		ctx, cancel := context.WithTimeout(rootCtx, 3*time.Second)
		defer cancel()
		return pool.Ping(ctx)
	}(); err != nil {
		log.Fatalf("База недоступна: %v", err)
	}
	log.Println("Ping OK — база отвечает")

	// 3) Подготовим базу (DDL). Можно держать это в миграциях (goose/tern/etc.),
	// тут — компактно для демонстрации.
	if err := func() error {
		ctx, cancel := context.WithTimeout(rootCtx, 5*time.Second)
		defer cancel()
		return pgx_demo.EnsureSchema(ctx, pool)
	}(); err != nil {
		log.Fatalf("ensureSchema: %v", err)
	}

	// 4) Регистрация/логин пользователя с транзакцией.
	email := "alice@example.com"
	name := "Alice"
	// middle_name может быть NULL — покажем работу pgtype.Text{Valid:false}
	var middleName *string // nil => NULL
	userID, err := pgx_demo.UpsertUserAndLogLogin(rootCtx, pool, email, name, middleName)
	if err != nil {
		log.Fatalf("upsertUserAndLogLogin: %v", err)
	}
	log.Printf("Пользователь id=%d готов\n", userID)

	// 5) Гарантируем аккаунт и читаем баланс (Numeric через pgtype).
	if err := pgx_demo.EnsureAccount(rootCtx, pool, userID); err != nil {
		log.Fatalf("ensureAccount: %v", err)
	}
	bal, err := pgx_demo.GetBalance(rootCtx, pool, userID)
	if err != nil {
		log.Fatalf("getBalance: %v", err)
	}
	log.Printf("Начальный баланс пользователя %d: %s\n", userID, bal.Int) // bal.String() уже человекочитаемый

	// 6) Пример acquire/release вручную — показываем, что можно брать соединение прямо из пула,
	// а не всегда через pool.Exec/Query*. Внутри Acquire пул проверит «живость» —
	// там используется внутренняя логика вида ShouldPing: если пора/нужно — будет Ping,
	// мертвые соединения закрываются и берется следующее.
	if err := pgx_demo.SampleAcquireRelease(rootCtx, pool); err != nil {
		log.Fatalf("sampleAcquireRelease: %v", err)
	}

	// 7) Работа с NULL и pgtype.* при сканировании.
	if err := pgx_demo.DemoScanWithPgtype(rootCtx, pool, email); err != nil {
		log.Fatalf("demoScanWithPgtype: %v", err)
	}

	// 8) Достанем FieldDescriptions() для метаданных запроса — имена и типы колонок результата.
	if err := pgx_demo.ShowQueryMetadata(rootCtx, pool); err != nil {
		log.Fatalf("showQueryMetadata: %v", err)
	}

	// 9) Метаданные prepared‑выражения без выполнения запроса: ParamOIDs и имена колонок результата.
	if err := pgx_demo.ShowPreparedStatementMetadata(rootCtx, pool); err != nil {
		log.Fatalf("prepared metadata: %v", err)
	}

	// 10) Пример tx.Query — итерация по Rows внутри транзакции с правильным закрытием/Err/Commit.
	if err := pgx_demo.TxQueryExample(rootCtx, pool); err != nil {
		log.Fatalf("tx query example: %v", err)
	}

	// 11) Демонстрация записи/чтения ограниченного набора типов с NULL (Valid=false → NULL):
	// I4 — зададим значение, остальные поля местами оставим NULL с Valid=false.
	sample := pgx_demo.TypeSample{
		UUID: pgtype.UUID{Valid: false},            // NULL
		I2:   pgtype.Int2{Valid: false},            // NULL
		I4:   pgtype.Int4{Int32: 42, Valid: true},  // 42
		I8:   pgtype.Int8{Valid: false},            // NULL
		Flag: pgtype.Bool{Bool: true, Valid: true}, // TRUE
		Note: pgtype.Text{Valid: false},            // NULL
		Num:  pgtype.Numeric{Valid: false},         // NULL (число показано на аккаунте выше)
		TS:   pgtype.Timestamp{Valid: false},       // NULL
	}
	sid, err := pgx_demo.InsertTypeSample(rootCtx, pool, sample)
	if err != nil {
		log.Fatalf("insert type sample: %v", err)
	}
	got, err := pgx_demo.GetTypeSample(rootCtx, pool, sid)
	if err != nil {
		log.Fatalf("get type sample: %v", err)
	}
	// Покажем, какие поля пришли NULL, а какие — с данными (через Valid)
	mid := "NULL"
	if got.Note.Valid {
		mid = got.Note.String
	}
	log.Printf("type_sample id=%d: UUID.Valid=%v I2.Valid=%v I4=%d I8.Valid=%v Flag=%v Note=%s Num.Valid=%v TS.Valid=%v",
		sid, got.UUID.Valid, got.I2.Valid, got.I4.Int32, got.I8.Valid, got.Flag.Bool, mid, got.Num.Valid, got.TS.Valid)

	// 12) Обработка ошибок БД: перехват *pgconn.PgError (уникальное нарушение 23505 для email).
	if err := pgx_demo.DemoPgErrorHandling(rootCtx, pool, email); err != nil {
		log.Fatalf("pg error handling: %v", err)
	}

	log.Println("Демонстрация завершена успешно")
}
