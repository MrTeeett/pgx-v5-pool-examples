// Демонстрация: pgxpool.ParseConfig + NewWithConfig, пинг, транзакции,
// prepared statements на уровне пула (через AfterConnect), pgtype для NULL,
// FieldDescriptions() для метаданных, и явная Acquire/Release работа.

package pgx_demo

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Имена подготовленных выражений (prepare) — мы будем готовить их
// из хука AfterConnect, чтобы каждое соединение пула имело одинаковый набор.
const (
	psInsertUser       = "ps_insert_user"
	psSetLastLogin     = "ps_set_last_login"
	psGetUserByEmail   = "ps_get_user_by_email"
	psGetUserIdByEmail = "ps_get_user_id_by_email"
	psEnsureAccount    = "ps_ensure_account"
	psGetBalance       = "ps_get_balance"
	psSelectUsersLight = "ps_select_users_light"
)

// bootstrapEnsureSchema подключается напрямую (без пула) и создаёт таблицы.
func BootstrapEnsureSchema(ctx context.Context, dsn string) error {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("parse bootstrap dsn: %w", err)
	}

	// cfg.ConnConfig — конфиг ОДНОГО соединения, в нём уже НЕТ pool_* параметров.
	conn, err := pgx.ConnectConfig(ctx, cfg.ConnConfig)
	if err != nil {
		return fmt.Errorf("bootstrap connect: %w", err)
	}
	defer conn.Close(ctx)

	ddl := []string{
		`CREATE TABLE IF NOT EXISTS app_users (
            id          BIGSERIAL PRIMARY KEY,
            email       TEXT UNIQUE NOT NULL,
            name        TEXT NOT NULL,
            middle_name TEXT,
            last_login  TIMESTAMPTZ,
            is_active   BOOLEAN NOT NULL DEFAULT TRUE
        )`,
		`CREATE TABLE IF NOT EXISTS accounts (
            user_id BIGINT PRIMARY KEY REFERENCES app_users(id) ON DELETE CASCADE,
            balance NUMERIC(12,2) NOT NULL
        )`,
	}
	for _, q := range ddl {
		if _, err := conn.Exec(ctx, q); err != nil {
			return fmt.Errorf("bootstrap DDL failed: %w (query=%s)", err, q)
		}
	}
	return nil
}

// buildPool: ParseConfig + тонкая настройка конфигурации пула.
// ВАЖНО: cfg.ConnConfig — это «конфиг одиночного соединения» (таймауты, user, dbname, ssl, прост/extended протокол и т.п.).
// Параметры пула (MaxConns/MinConns/MaxConnLifetime/MaxConnIdleTime/HealthCheckPeriod + хуки)
// лежат "рядом", но не в ConnConfig.
func BuildPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("ParseConfig: %w", err)
	}

	// Пример тюнинга пула (можно переопределить то, что пришло из DSN):
	cfg.MaxConns = 10                      // верхний предел одновременных соединений
	cfg.MinConns = 2                       // минимально поддерживаемое количество
	cfg.MaxConnLifetime = 30 * time.Minute // «возраст» соединения: после этого его лучше пересоздать
	cfg.MaxConnIdleTime = 5 * time.Minute  // максимум простоя до закрытия
	cfg.HealthCheckPeriod = time.Minute    // фоновая проверка живости коннектов

	// Хук AfterConnect сработает на только что созданном соединении.
	// Идеально подходит, чтобы «унифицировать» каждое соединение (SET'ы, prepared statements и т.п.).
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		// Для примера — проставим application_name, чтобы видеть в pg_stat_activity.
		if _, err := conn.Exec(ctx, "set application_name = 'pgxpool-demo'"); err != nil {
			return err
		}

		// Готовим ключевые выражения. Подготовленное выражение привязано к КОНКРЕТНОМУ соединению.
		// Благодаря AfterConnect мы гарантируем, что каждое соединение пула его имеет.
		if _, err := conn.Prepare(ctx, psInsertUser,
			`INSERT INTO app_users(email, name, middle_name)
             VALUES ($1,$2,$3)
             ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name
             RETURNING id`); err != nil {
			return err
		}
		if _, err := conn.Prepare(ctx, psSetLastLogin,
			`UPDATE app_users SET last_login = now() WHERE id = $1`); err != nil {
			return err
		}
		if _, err := conn.Prepare(ctx, psGetUserByEmail,
			`SELECT id, email, name, middle_name, last_login, is_active
			   FROM app_users
			  WHERE email = $1`); err != nil {
			return err
		}
		if _, err := conn.Prepare(ctx, psEnsureAccount,
			`INSERT INTO accounts(user_id, balance)
             VALUES ($1, 0)
             ON CONFLICT (user_id) DO NOTHING`); err != nil {
			return err
		}
		if _, err := conn.Prepare(ctx, psGetBalance,
			`SELECT balance FROM accounts WHERE user_id = $1`); err != nil {
			return err
		}
		if _, err := conn.Prepare(ctx, psSelectUsersLight,
			`SELECT id, email, name FROM app_users ORDER BY id LIMIT 5`); err != nil {
			return err
		}
		if _, err := conn.Prepare(ctx, psGetUserIdByEmail,
			`SELECT id FROM app_users WHERE email = $1`); err != nil {
			return err
		}
		return nil
	}

	// Хук BeforeAcquire — можно добавить легкие проверки/фильтры перед выдачей соединения.
	cfg.BeforeAcquire = func(ctx context.Context, conn *pgx.Conn) bool {
		// Возвращаем true — «соединение годится».
		// Здесь можно, например, проверять свойства сессии.
		return true
	}

	// Хук AfterRelease — трекинг/логирование момента возврата.
	cfg.AfterRelease = func(conn *pgx.Conn) bool {
		// Возвращаем true — «соединение оставить в пуле».
		// Можно вернуть false, если хотим закрыть это соединение (например, заметили подозрительное состояние).
		return true
	}

	// Итог: cfg содержит как ConnConfig (настройка одного соединения),
	// так и параметры пула (лимиты/хуки/политики возраста-простоя).
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("NewWithConfig: %w", err)
	}
	return pool, nil
}

// ensureSchema — создаем минимальную схему для примеров.
// В реальном проекте это переносится в миграции.
func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	ddl := []string{
		`CREATE TABLE IF NOT EXISTS app_users (
			id          BIGSERIAL PRIMARY KEY,
			email       TEXT UNIQUE NOT NULL,
			name        TEXT NOT NULL,
			middle_name TEXT,
			last_login  TIMESTAMPTZ,
			is_active   BOOLEAN NOT NULL DEFAULT TRUE
		)`,
		`CREATE TABLE IF NOT EXISTS accounts (
			user_id BIGINT PRIMARY KEY REFERENCES app_users(id) ON DELETE CASCADE,
			balance NUMERIC(12,2) NOT NULL
		)`,
	}
	for _, q := range ddl {
		if _, err := pool.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

// upsertUserAndLogLogin — реальный шаблон работы с транзакцией:
// 1) UPSERT пользователя (email — естественный уникальный ключ).
// 2) Логируем вход (обновляем last_login).
// Все методы Tx принимают context — это важно для таймаутов и отмены.
func UpsertUserAndLogLogin(ctx context.Context, pool *pgxpool.Pool, email, name string, middleName *string) (int64, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) // безопасно вызвать повторно — откатится только если не был Commit

	// Подготовленные выражения, сделанные в AfterConnect, доступны и из tx:
	// Вызов tx.QueryRow(ctx, "ps_name", ...) — это ВЫЗОВ ПО ИМЕНИ prepared-statement.
	var mid pgtype.Text
	if middleName != nil {
		mid = pgtype.Text{String: *middleName, Valid: true}
	} else {
		mid = pgtype.Text{Valid: false} // => запись NULL в колонку middle_name
	}

	var userID int64
	if err := tx.QueryRow(ctx, psInsertUser, email, name, mid).Scan(&userID); err != nil {
		return 0, err
	}

	if _, err := tx.Exec(ctx, psSetLastLogin, userID); err != nil {
		return 0, err
	}

	// Важно: Commit/rollback возвращают соединение в пул.
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return userID, nil
}

// ensureAccount — «лениво» создаем счет при первом заходе пользователя.
func EnsureAccount(ctx context.Context, pool *pgxpool.Pool, userID int64) error {
	_, err := pool.Exec(ctx, psEnsureAccount, userID)
	return err
}

// getBalance — читаем NUMERIC в pgtype.Numeric для корректной работы с точностью/NaN/Inf.
func GetBalance(ctx context.Context, pool *pgxpool.Pool, userID int64) (pgtype.Numeric, error) {
	var n pgtype.Numeric
	if err := pool.QueryRow(ctx, psGetBalance, userID).Scan(&n); err != nil {
		return pgtype.Numeric{}, err
	}
	if !n.Valid {
		return pgtype.Numeric{}, errors.New("balance is NULL — для примера считаем это ошибкой")
	}
	return n, nil
}

// sampleAcquireRelease — ручное получение и возврат соединения.
// Показывает: Acquire → работа с *pgxpool.Conn → Release.
// Если соединение «подвисло», логика выдачи в пуле может решить, что его нужно пинговать
// (внутренняя ShouldPing) или вовсе уничтожить и взять другое.
func SampleAcquireRelease(ctx context.Context, pool *pgxpool.Pool) error {
	c, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer c.Release()

	// На уровне "сырых" соединений можно выполнять любые команды.
	var now time.Time
	if err := c.QueryRow(ctx, "select now()").Scan(&now); err != nil {
		return err
	}
	log.Printf("Acquire/Release: now() = %s", now.Format(time.RFC3339))
	return nil
}

// demoScanWithPgtype — демонстрация сканирования с pgtype.* и проверкой Valid (NULL-safe).
func DemoScanWithPgtype(ctx context.Context, pool *pgxpool.Pool, email string) error {
	// Получим данные по пользователю с использованием prepared-select.
	var (
		id         int64
		em         string
		name       string
		middleName pgtype.Text      // NULL-safe текст
		lastLogin  pgtype.Timestamp // NULL-safe timestamp с поддержкой InfinityModifier
		isActive   pgtype.Bool
	)
	if err := pool.QueryRow(ctx, psGetUserByEmail, email).
		Scan(&id, &em, &name, &middleName, &lastLogin, &isActive); err != nil {
		return err
	}

	// Проверяем Valid — если false, в БД был NULL.
	mid := "NULL"
	if middleName.Valid {
		mid = middleName.String
	}
	ll := "NULL"
	if lastLogin.Valid {
		ll = lastLogin.Time.Format(time.RFC3339)
	}
	act := "NULL"
	if isActive.Valid {
		act = fmt.Sprintf("%v", isActive.Bool)
	}

	log.Printf("User: id=%d email=%s name=%s middle_name=%s last_login=%s is_active=%s",
		id, em, name, mid, ll, act)
	return nil
}

// showQueryMetadata — получение метаданных результата.
// Rows.FieldDescriptions() возвращает срез pgconn.FieldDescription (имя колонки, OID типа и т.д.).
func ShowQueryMetadata(ctx context.Context, pool *pgxpool.Pool) error {
	rows, err := pool.Query(ctx, psSelectUsersLight)
	if err != nil {
		return err
	}
	defer rows.Close()

	fds := rows.FieldDescriptions()
	var names []string
	for _, fd := range fds {
		// fd.Name — []byte
		names = append(names, string(fd.Name))
	}
	log.Printf("Метаданные: колонки результата = [%s]", strings.Join(names, ", "))

	// Пройдемся по строкам как обычно (показываем, что доступ к данным «обычный»).
	for rows.Next() {
		var id int64
		var email, name string
		if err := rows.Scan(&id, &email, &name); err != nil {
			return err
		}
		log.Printf("row: id=%d email=%s name=%s", id, email, name)
	}

	// Проверяем ошибки курсора.
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

// (пояснение по пингу)
// При выдаче соединения из пула внутри pgx решается, нужно ли делать Ping именно сейчас —
// это завязано на времени «последнего использования», HealthCheckPeriod и т.п.
// Если соединение не отвечает, пул пометит его «битым», уничтожит и попробует следующее.
// Отдельно метод Pool.Ping(ctx) делает явный ping (Acquire → Conn.Ping → Release).
//
// Примечание: прямого поля ShouldPing в конфиге нет — это внутренняя логика пула.
// Мы демонстрируем эффекты через HealthCheckPeriod + Ping.
