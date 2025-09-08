# pgx v5 pool examples (Go)

Практическая демонстрация idiomatic-подходов при работе с PostgreSQL через `pgx` v5 и пул соединений `pgxpool`.

Что показываем на реальном коде:
- Создание пула из DSN с `pgxpool.ParseConfig` → `pgxpool.NewWithConfig`, тонкая настройка (лимиты, таймауты, health-check, хуки).
- `AfterConnect` для унификации каждого соединения пула (SET-параметры, подготовленные выражения).
- Явный `Ping` после старта пула — проверка доступности БД.
- Транзакции с контекстами: `Begin` → `Query/QueryRow/Exec` → `Commit/Rollback`.
- Работа с NULL-safe типами `pgtype.*` (Text, Int2/4/8, UUID, Bool, Numeric, Timestamp) — флаг `Valid`.
- Метаданные результатов: `Rows.FieldDescriptions()` и метаданные prepared-выражений через `StatementDescription`.
- Acquire/Release «сырых» соединений из пула.
- Микро-бенчмарки: издержки acquire/release и выигрыш от prepared.

Структура
- `main.go` — сценарий демонстрации, таймауты контекстов, пинг, вызовы примеров.
- `pgx_demo/pgx_demo.go` — реальная логика: конфигурация пула, хуки, prepared, транзакции, pgtype, метаданные, обработка PgError.
- `pgx_demo/bench_test.go` — микро-бенчмарки (Go `testing` benchmarks).

Системные требования
- PostgreSQL доступный по DSN в переменной окружения `PGURL`.
- Go 1.20+ (проект использует v5 `pgx`).

Быстрый старт
1) Экспортируйте DSN или используйте дефолт из `main.go` (локальный Postgres):

```
export PGURL='postgres://myadmin:masterkey@localhost:5432/app?sslmode=disable&pool_max_conns=10&pool_min_conns=2&pool_min_idle_conns=1'
```

2) Запуск демо:

```
go run .
```

Ожидаемые шаги во время запуска:
- Bootstrap DDL создаст таблицы (`app_users`, `accounts`, `type_samples`).
- Поднимется пул соединений, выполнится `Ping`.
- Выполнится upsert пользователя, транзакционный пример, примеры pgtype/NULL, метаданные запросов и prepared, обработка `PgError`.

3) Бенчмарки:

```
go test ./pgx_demo -bench=. -benchmem
```

Подключение и пул
- Ключевая функция: `pgx_demo.BuildPool` (`pgx_demo/pgx_demo.go`).
  - `pgxpool.ParseConfig(dsn)` — парсит строку подключения и РАЗДЕЛЯЕТ «параметры пула» (`pool_max_conns`, `pool_min_conns`, `pool_min_idle_conns`, и др.) и конфиг одиночного соединения `ConnConfig`.
  - `pgxpool.NewWithConfig(ctx, cfg)` — создаёт пул с лимитами, таймаутами, health-check периодом и хуками.
  - Настройки, которые демонстрируются: `MaxConns`, `MinConns`, `MaxConnLifetime`, `MaxConnIdleTime`, `HealthCheckPeriod`.
- Хуки:
  - `AfterConnect` — выполняется на только что созданном соединении: ставим `application_name` и регистрируем подготовленные выражения (они привязываются к конкретному соединению).
  - `BeforeAcquire` — фильтрация/проверки перед выдачей соединения из пула.
  - `AfterRelease` — возможность закрыть/оставить соединение после возврата.
- Пинг базы: в `main.go` вызов `pool.Ping(ctx)` с коротким таймаутом — быстрый индикатор доступности.

Полезные исходники в pgx:
- Pool config/создание пула: https://github.com/jackc/pgx/blob/master/pgxpool/pool_test.go
  - Обратите внимание на `TestConnectConfig`, `TestParseConfigExtractsPoolArguments`.
- Бенчмарки пула: https://github.com/jackc/pgx/blob/master/pgxpool/bench_test.go

Транзакции (всё с контекстом)
- Upsert + лог входа: `pgx_demo.UpsertUserAndLogLogin` — шаблон «начал транзакцию → сделал несколько действий → commit/rollback».
  - Важно: `defer tx.Rollback(ctx)` безопасно откатывает только если не было `Commit`.
  - Prepared-выражения, определённые в `AfterConnect`, доступны и из транзакции (вызов по имени prepared).
- Итерация по `Rows` внутри транзакции: `pgx_demo.TxQueryExample`.
  - Корректная последовательность: `tx.Query` → `rows.Next/Scan` → `rows.Err()` → `rows.Close()` → `tx.Commit()`.
- Дополнительно: в `main.go` для критичных операций используются `context.WithTimeout` — стандартная защита от зависаний при сетевых проблемах.

Типы данных и NULL (pgtype)
- В pgx v5 используются структуры вида `type T struct { <value>; Valid bool }` — если `Valid=false`, значение кодируется/читается как SQL `NULL`.
- Покрытые типы в примерах: `pgtype.Text`, `pgtype.Int2`, `pgtype.Int4`, `pgtype.Int8`, `pgtype.UUID`, `pgtype.Bool`, `pgtype.Numeric`, `pgtype.Timestamp`.
- Что смотреть:
  - `pgx_demo.DemoScanWithPgtype` — чтение пользователя с `Text/Timestamp/Bool` и проверкой `Valid`.
  - `pgx_demo.TypeSample` — модель строки для таблицы `type_samples` с полями `pgtype.*`.
  - `pgx_demo.InsertTypeSample` / `pgx_demo.GetTypeSample` — запись/чтение значений, включая `NULL` через `Valid=false`.
  - Особенность `Numeric`: используется `pgtype.Numeric` для корректной точности/масштаба.

Справочники по типам в pgx:
- Каталог типов: https://github.com/jackc/pgx/tree/master/pgtype
- Базовые интерфейсы/описание: https://github.com/jackc/pgx/blob/master/pgtype/pgtype.go

Метаданные запросов
- По результату запроса: `pgx_demo.ShowQueryMetadata` использует `Rows.FieldDescriptions()` для получения имён колонок (и OID типов, если нужно).
  - Интерфейс `Rows`: https://github.com/jackc/pgx/blob/master/rows.go
- По prepared-выражениям: `pgx_demo.ShowPreparedStatementMetadata` делает `Conn.Prepare(ctx, "", sql)` и использует `StatementDescription`:
  - `ParamOIDs` — OID типов параметров.
  - `Fields` — описание колонок результата.
  - Низкоуровневый pgconn: https://github.com/jackc/pgx/blob/master/pgconn/pgconn.go
  - Тесты и примеры: https://github.com/jackc/pgx/blob/master/pgconn/pgconn_test.go

Prepared statements и AfterConnect
- Подготовленные выражения привязаны к конкретному соединению. Чтобы каждое соединение пула имело одинаковый набор prepared, они регистрируются в `AfterConnect`.
- Вызов по имени: `pool.QueryRow(ctx, psName, args...)` или `tx.QueryRow(ctx, psName, args...)`.
- В примере готовятся выражения для `users`, `accounts`, и отдельной таблицы `type_samples`.

Acquire/Release
- `pgx_demo.SampleAcquireRelease` — берём соединение через `pool.Acquire(ctx)`, работаем на уровне `*pgx.Conn`, затем `Release()`.
- Пул может выполнять фоновый health-check (`HealthCheckPeriod`) и пинговать/пересоздавать «подвисшие» соединения.
  - Внутренняя логика «когда пинговать» — внутри pgx; прямого поля `ShouldPing` нет.

Обработка ошибок Postgres
- `pgx_demo.DemoPgErrorHandling` — перехват `*pgconn.PgError` (пример `unique_violation` 23505 при нарушении уникального индекса).
- Полезно логировать `Code`, `Message`, `Detail`, `ConstraintName` и ветвить логику по коду.

Модель данных (минимальная)
- `app_users` — пользователи (email — уникален), хранится `last_login`, допускается `middle_name IS NULL`.
- `accounts` — счёт пользователя (создаётся лениво при первом заходе), `NUMERIC(12,2)`.
- `type_samples` — отдельная таблица для демонстрации `pgtype.*` и `NULL`.

Структура репозитория
- Исходники:
  - `main.go`
  - `pgx_demo/pgx_demo.go`
  - `pgx_demo/bench_test.go`
