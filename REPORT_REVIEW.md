# Финальное ревью ветки Go-порта (d505cfb → 1f3556d)

## 1. Итог

**Ready to merge: With fixes.** Сборка, линтеры, unit-, интеграционные и сквозные тесты, Docker-образ и CI-файлы в порядке.
Перед мержем нужно исправить 4 Important. Critical нет.

| Severity | Кол-во | ID |
|---|---|---|
| Critical | 0 | — |
| Important | 4 | F1, F2, F3, F4 |
| Minor | 10 | F5–F14 |

Открытый вопрос из `deferred-and-rulings.md` («выходит ли `run` с кодом 1 при штатном SIGTERM из-за `context.Canceled`») **закрыт: нет**, доказано тестом (п. 2, строка 6b).

CI на GitHub Actions ([run 37554796290](https://github.com/a1ekseev/chat-analyze/actions/runs/37554796290)) для head 1f3556d: lint ✅, unit ✅, vuln ✅, integration ✅, docker ✅.

---

## 2. Таблица проверок

Окружение: `GOTOOLCHAIN=go1.27.1`, `$(go env GOPATH)/bin/golangci-lint` v2.14.0 (собран go1.27.1), Docker 29.8.2. Локальные базовые образы уже были. Postgres для тестов: отдельный контейнер `rev-pg` на :55432 (tmpfs), после проверок удалён.

| # | Команда | Результат | Вывод |
|---|---|---|---|
| 1 | `gofmt -l .` | ✅ | пусто |
| 1 | `go vet ./...` / `go vet -tags=integration ./...` | ✅ | rc=0 / rc=0 |
| 1 | `go mod tidy -diff` | ✅ | rc=0, без диффа |
| 1 | `go generate ./... && git status --porcelain` | ✅ | дерево чистое |
| 1 | `golangci-lint fmt --diff` | ✅ | rc=0 |
| 2 | `golangci-lint run ./...` | ✅ | `0 issues.` |
| 2 | `golangci-lint run --build-tags=integration ./...` | ✅ | `0 issues.` |
| 3 | `go test -race -count=1 ./...` | ✅ | 10 пакетов `ok` |
| 3 | `TEST_DATABASE_URL=postgres://postgres:test@127.0.0.1:55432/test?sslmode=disable go test -tags=integration -race -count=1 ./...` | ✅ | все пакеты `ok` |
| 3 | то же без `TEST_DATABASE_URL` | ✅ | падает: `TEST_DATABASE_URL is required for integration tests` |
| 3 | `just test-integration` | ✅ | rc=0, 2м27с, все пакеты `ok` в контейнере `tests` |
| 3 | `go test -tags=integration -v -run TestE2E ./internal/e2e/` | ✅ | 6/6 PASS, включая `TestE2E_NoMessageDataInLogs` |
| 4 | `docker build --target runtime --build-arg VERSION=review --build-arg COMMIT=1f3556d -t chat-analyze:review .` | ✅ | собран |
| 4 | `docker run --rm chat-analyze:review version` | ✅ | `review 1f3556d`, rc=0 |
| 4 | `docker run --rm chat-analyze:review healthcheck` (никто не слушает) | ✅ | rc=1 |
| 4 | `docker image inspect` | ✅ | `user=nonroot:nonroot`, entry `/chat-analyze`, cmd `run`, HEALTHCHECK 30s/5s/30s, 71.5MB на диске (~16MB контент) |
| 4 | `docker run … run` без env; `migrate bogus` | ✅ | JSON `config invalid` (`invalid config: DATABASE_URL, TG_API_HASH, TG_API_ID`), rc=1; usage, rc=2 |
| 5 | `docker compose config -q` (с временным `.env`) | ✅ | rc=0 (без `.env` — `env file … not found`, ожидаемо по README) |
| 5 | `docker compose up -d postgres` → `docker compose run --rm migrate` | ✅ | `db version 3` |
| 5 | `migrate migrate status` → `down` → `up`; `just migrate status` | ✅ | 3 → 2 → 3; 3 |
| 5 | `docker compose up -d app`, 45с наблюдения, без сессии | ❌ | **не падает быстро**: бесконечно `"msg":"Got error on self","component":"gotd","error":"wait for reconnect: context canceled"`, health 503 `telegram:not_ready`, ошибки `session not found` нет → **F3** |
| 5 | `docker compose stop -t 40 app` | ⚠️ | 10с = `SHUTDOWN_TIMEOUT`, `shutdown timed out`, exit=1 (Ruling 16; уходит вместе с F3) |
| 5 | grep логов app/migrate на `Pw0CANARY` (пароль БД), `deadbeefcafebabe` (TG_API_HASH), `9991234567` (телефон) | ✅ | 0 совпадений; все строки — валидный JSON, `msg` статические |
| 5 | Неверный пароль / неизвестный хост / битый DSN для `run` и `migrate` | ✅ | `failed SASL auth: pg error 28P01 ()`, `no such host`, `parse database config: invalid DSN`; пароля нет |
| 5 | `docker compose -f docker-compose.test.yml up -d postgres` при поднятом основном стеке, затем `down -v` | ❌ | `Container chat-analyze-postgres-1 Recreated` (теперь tmpfs); `down -v` удалил его и сеть `chat-analyze_default` → **F4** |
| 5 | Очистка (`compose down -v`, `rm .env`, `docker rm -f rev-pg`) | ✅ | `git status --porcelain` пуст |
| 6 | gotd `telegram/connect.go` (`Run`: `if err := g.Wait(); !errors.Is(err, context.Canceled) { return err }; return nil`) + `internal/telegram/client.go:155` (`if ctx.Err() != nil { return nil }`) | ✅ | двойная защита: штатная отмена → `nil` → `runStages` не пишет `firstErr` → exit 0 |
| 6b | Тест-доказательство в scratchpad (`sigterm-proof/proof_test.go`): gotd `tgtest/cluster` + `telegram.NewClient`, колбэк ждёт `<-ctx.Done()` и возвращает `ctx.Err()` (как `updates.Manager.Run`), затем `cancel()` | ✅ | `Run returned <nil> after 112µs; errors.Is(Canceled)=false`, PASS |
| 7 | grep `fmt.Errorf`/`errors.New`/`%v %s %q`/`slog.String` по prod-коду | ✅ | в ошибки попадают только операции и числовые id; `%q` только для константных значений |
| 7 | Мутация: `SanitizeError` → `return err.Error()`, прогон `TestE2E_NoMessageDataInLogs` | ❌ | тест всё равно PASS → e2e-часть канарейки про ошибку БД вакуумна → **F7** (unit-тесты logx покрывают санитайзер) |
| 7 | Рандомизированный no-panic тест `Markdown`/`EntitiesJSON` (300k случаев) | ✅ | паник нет |
| 7 | `upsertSQL` в psql: сначала правка (updated_at=00:05, `EDITED`), затем оригинал (updated_at NULL, `ORIGINAL`) | ❌ | итог `ORIGINAL \| 2026-01-01 00:05:00+00` → **F1** |
| 7 | `enqueueSQL`+`claimSQL`: Claim(v1) → Enqueue → Claim | ❌ | второй Claim выдаёт тот же id=1 (v2), пока v1 ещё у воркера → **F1** |
| 8 | `go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12 .github/workflows/ci.yml` | ✅ | rc=0 |
| 8 | `git ls-remote --tags` всех actions | ✅ | существуют: checkout v7, setup-go v7, upload-artifact v7, golangci-lint-action v9, setup-just v4, setup-buildx v4, metadata v6, login v4, build-push v7 |
| 9 | Review Focus 1–5 | ⚠️ | все 8 тестов есть и зелёные; п.4 покрыт только «старая правка после новой», случай «оригинал после правки» не тестируется и ломается (F1) |
| 10 | `govulncheck` | ⚠️ | локально заблокирован (vuln.go.dev 403), покрывается job `vuln` в CI (зелёный) |
| — | История git на секреты | ✅ | `8eb6b10:build/app/session.py` содержит `***REMOVED***` — история переписана, README прав |
| — | Прокси/CA в трекаемых файлах | ✅ | нет; `.superpowers/` не трекается |

---

## 3. Findings

### Critical
Нет.

### Important

#### F1. Оригинал, обработанный после правки, откатывает правку; при `WORKERS>1` одно задание обрабатывается параллельно
- **Файлы:** `internal/message/repo.go:44` (WHERE upsert), `internal/queue/queue.go:59` (Enqueue переводит `running` → `pending`), тест `internal/message/repo_integration_test.go:52`.
- **Что не так:** условие `WHERE messages.updated_at IS NULL OR EXCLUDED.updated_at IS NULL OR …` пропускает неотредактированный оригинал поверх сохранённой правки. Строка становится противоречивой: текст старый, а `updated_at` — время правки (`COALESCE`). Это SQL из spec §7 дословно, но он противоречит заявленной там же цели и Review Focus п.4.
  Как это происходит: `Enqueue` ставит `status='pending'` даже заданию в статусе `running`. При `WORKERS>1` второй воркер сразу берёт ту же строку (v2), пока первый пишет v1, и порядок коммитов произвольный. При `WORKERS=1` (по умолчанию) так не бывает, но до 64 — штатная настройка из spec §9.
- **Воспроизведение (psql):**
  - Схема из `00001_messages.sql`, `PREPARE up(...) AS <upsertSQL>`.
  - `EXECUTE up(..., 'EDITED', ..., '2026-01-01T00:05:00Z')`, затем `EXECUTE up(..., 'ORIGINAL', ..., NULL)`.
  - `SELECT message, updated_at FROM messages` → `ORIGINAL | 2026-01-01 00:05:00+00`.
  - Для очереди: `enq(v1)`, `claim`, `enq(v2)`, `claim` → второй claim выдаёт тот же id=1.
- **Как исправить:**
  1. Заменить условие:
     ```sql
     WHERE COALESCE(EXCLUDED.updated_at, '-infinity'::timestamptz) >= COALESCE(messages.updated_at, '-infinity'::timestamptz)
     ```
     Отклонение от буквы spec пояснить комментарием.
  2. Добавить тест `TestUpsert_OriginalAfterEditDoesNotOverwrite`.
  3. Желательно: интеграционный тест «Claim → Enqueue того же ключа → второй Claim», фиксирующий, что двойная выдача безопасна благодаря п.1.

#### F2. Неудачный backfill после `ChannelTooLong` навсегда теряет сообщения
- **Файлы:** `internal/telegram/listener.go:158-246`.
- **Что не так:** gotd при `channelDifferenceTooLong` сразу ставит pts канала в удалённое значение (`gotd/td@v0.162.0/telegram/updates/state_channel.go:409-425`). Пропуск больше не придёт апдейтами, единственный путь — наш `Backfill`. А он:
  - при любой ошибке `MessagesGetHistory` (FLOOD_WAIT вероятен при сотнях запросов без пауз; кратковременный обрыв сети) ничего не ставит в очередь и не повторяет попытку;
  - при остановке сервиса теряет уже скачанное: `Enqueue` вызывается с отменённым root ctx.

  Посылка Ruling 13 («следующий backfill повторит с того же LastMessageID») неверна. Новый backfill будет только при новом TooLong, а к тому времени живые сообщения уже сдвинут `LastMessageID` за пропуск. Потеря молчаливая: одна строка `backfill failed`.
- **Воспроизведение:** unit-тест по образцу `TestListener_BackfillFetchErrorEnqueuesNothing`: первый вызов → `tgerr.New(420,"FLOOD_WAIT_3")`, второй → успех. Сейчас повторного вызова нет и в очередь ничего не попадает.
- **Как исправить:**
  1. Повторять `Backfill` с backoff (например, 5 попыток, `queue.Backoff`). При `tgerr.AsFloodWait` ждать `d+1s` в `select` с `ctx.Done()`.
  2. Фиксировать `last` (MinID) один раз до первой попытки и передавать его в повторы.
  3. Надёжнее: задание `kind="backfill_channel"` с payload `{channel_id, min_id}` и dedup по каналу. Тогда повторы, FLOOD_WAIT, переживание рестарта и reap работают через существующую очередь (нужен обработчик нового kind в worker).
  4. Тесты на FLOOD_WAIT → успех и на неизменный MinID при повторе.

#### F3. `run` без сессии не падает быстро с `session not found`
- **Файлы:** `internal/telegram/client.go:120-160`.
- **Что не так:** spec §9 и README обещают ошибку `session not found, run "chat-analyze login"`. Сейчас `ErrNoSession` возвращается только после подключения к Telegram, создания нового auth key и `Auth().Status`. Последствия:
  - если Telegram недоступен, сервис висит бесконечно с `Got error on self`, health 503, о login ни слова;
  - если доступен, каждый запуск без сессии создаёт auth key на стороне Telegram и пишет неавторизованную сессию в `tg_session`;
  - остановка в этом состоянии упирается в `SHUTDOWN_TIMEOUT` и даёт exit 1 (Ruling 16).
- **Воспроизведение:**
  1. `.env` из `.env.example` с `TG_API_ID=12345` и `TG_API_HASH` из 32 hex-символов.
  2. `docker compose up -d postgres && docker compose run --rm migrate && docker compose up -d app`.
  3. `sleep 45; docker compose logs app` → `session not found` нет.
  4. `docker compose stop app` → 10с, `shutdown timed out`, exit=1.
- **Как исправить:**
  - В начале `Client.Run`, до `c.raw.Run`, вызвать `SessionStorage.LoadSession(ctx)`; хранить `*SessionStorage` в `Client`.
  - На `session.ErrNotFound` залогировать `telegram auth required` и вернуть `ErrNoSession`; прочие ошибки оборачивать.
  - Integration-тест: пустой `tg_session` → `ErrNoSession` быстрее 1с без сети.
  - Тест на `app.Run`: ошибка удовлетворяет `errors.Is(err, telegram.ErrNoSession)`.

#### F4. `docker-compose.test.yml` использует то же имя проекта, что и основной стек
- **Файлы:** `docker-compose.test.yml` (нет `name:`), `justfile:16-22`.
- **Что не так:** оба файла дают проект `chat-analyze` и оба описывают сервис `postgres`. `just test-integration` (и `just ci`) при поднятом `just up` пересоздаёт `chat-analyze-postgres-1` с tmpfs и тестовыми кредами. Затем `down -v` удаляет этот контейнер и сеть `chat-analyze_default`. Том `pgdata` уцелеет, но сервис ляжет.
- **Воспроизведение:**
  1. `docker compose -f docker-compose.test.yml config | grep '^name:'` и `docker compose config | grep '^name:'` → оба `chat-analyze`.
  2. `docker compose up -d postgres; docker compose -f docker-compose.test.yml up -d postgres` → `Recreated`.
  3. `docker compose -f docker-compose.test.yml down -v` → контейнер и сеть удалены.
- **Как исправить:** первой строкой `docker-compose.test.yml` добавить `name: chat-analyze-test` (или `-p chat-analyze-test` в обоих вызовах в justfile). Проверить повтором сценария.

### Minor

#### F5. `docker-compose.yml:33`: `environment: WORKERS: "1"` перекрывает `WORKERS` из `.env`
`.env` с `WORKERS=4` → `docker compose config app` показывает `WORKERS: "1"`. **Фикс:** удалить блок `environment` у `app`.

#### F6. Таймауты остановки не согласованы с Docker
- `SHUTDOWN_TIMEOUT=30s` больше grace period `docker compose stop` (10s), поэтому SIGKILL приходит раньше.
- `JOBS_LEASE=2m` (таймаут обработки задания) ещё длиннее.
- `SHUTDOWN_TIMEOUT<=0` не валидируется → остановка всегда «timed out» с exit 1.

**Фикс:** `stop_grace_period: 40s` у `app`; в config требовать `ShutdownTimeout >= 1s` и добавить тест; в README отметить, что таймаут должен быть меньше grace period оркестратора.

#### F7. Канареечный e2e-тест вакуумен для ошибки БД (`internal/e2e/e2e_integration_test.go:284`)
CHECK-нарушение кладёт значение только в `Detail`, а `PgError.Error()` его не включает. Мутация `SanitizeError = err.Error()` оставляет тест зелёным. **Фикс:** в `brokenStore` для `badID` выполнять запрос, где значение попадает в `Message`, например `SELECT $1::int` с canary → `invalid input syntax for type integer: "CANARY-7f3a"`; ошибку оборачивать `%w`. Проверить мутацией, что тест краснеет.

#### F8. Backfill канала без сохранённых сообщений тянет всю историю (`listener.go:186-232`)
При отсутствии строк `MinID=0`: до 100k сообщений, каждое в очередь и в mark read; почти наверняка FLOOD_WAIT. **Фикс:** при `!found` брать одну страницу или пропускать с `backfill skipped reason=no_history`; добавить тест.

#### F9. Compose собирает три одинаковых образа без версии
`build: .` без `image:` у migrate/app/login → три образа. В логе `service starting` всегда `version=dev commit=none`. **Фикс:** у всех трёх `image: chat-analyze:local`, у одного `build.args` VERSION/COMMIT; в `just up` передавать commit.

#### F10. `just lint` без `--build-tags=integration` (`justfile:10-11`), в отличие от CI
**Фикс:** `golangci-lint run --build-tags=integration`.

#### F11. Дрейф каталога событий spec §11.4
- Компонент `telegram_listener` (`listener.go:56`) не входит в список spec.
- Ключ `edit` вместо `is_edit` (`listener.go:145,149`). В `update received` нет `update_type`, `chat_type`, `is_service`.
- Нет `job enqueued`.
- `backfill done` пишет `messages` вместо `fetched`, нет `from_message_id` и `duration_ms`.
- `panic recovered` в `Listener.guard` без типа паники.

**Фикс:** привести к spec или задокументировать отличия в README.

#### F12. Пул соединений не масштабируется с `WORKERS` (`internal/db/pool.go:33`)
По умолчанию `MaxConns = max(4, NumCPU)`, одно соединение навсегда занимает Notifier. **Фикс:** если в DSN нет `pool_max_conns`, ставить `MaxConns >= WORKERS+4` через `PoolOption`; либо документировать `pool_max_conns`.

#### F13. Upsert затирает известное имя чата NULL'ом (`repo.go:38`)
`chat_name = EXCLUDED.chat_name` и `chat_username` без `COALESCE`. Если поиск пира по ошибке ничего не нашёл (deferred T12), правка обнуляет имя. **Фикс:** `COALESCE(EXCLUDED.chat_name, messages.chat_name)`, то же для `chat_username`; добавить тест.

#### F14. Задания `dead` никогда не чистятся
`cleanupSQL` удаляет только `done`; `dead` с payload (текст сообщения) копятся бессрочно. **Фикс:** отдельный срок хранения (`JOBS_DEAD_RETENTION`, например 30d) или явная оговорка в README.

---

## 4. Triage отложенных minor

| Задача | Minor | Решение | Почему |
|---|---|---|---|
| T1 | forbidigo cmd/ no-op; backtick `commit`; тонкие тесты; PATH | ok-to-leave | косметика; `\|\| echo none` работает (проверено `just build`) |
| T2 | неизвестный LOG_FORMAT → json | ok-to-leave | `oneof` в config не пропустит |
| T2 | сплющенная через `%v` pg-ошибка | ok-to-leave | grep: везде `%w` |
| T2 | `log.Any("error")` от gotd отбрасывается | ok-to-leave | безопасно |
| T2 | строка ошибки gotd только обрезается | ok-to-leave | там коды RPC/транспорта |
| T2 | fail-closed с обёртками; typed-nil `*PgError`; Err() из первой ошибки Join | ok-to-leave | безопасная сторона; pgx такого не возвращает; текст санитизирован целиком |
| T3 | e164 без `+`, лишние проверки, нет тестов Load() | ok-to-leave | поведение корректно |
| T4 | нет session lock у goose; `status` создаёт таблицу; прочее | ok-to-leave | compose упорядочивает migrate→app; для нескольких реплик стоит `WithSessionLocker` |
| T5 | нет теста отката Enqueue; `Fail(nil)`; OldestPendingAge с будущим `run_at` | ok-to-leave | `Fail(nil)` не вызывается; метрика лишь грубее |
| T6 | SetChannelPts без state и прочее | ok-to-leave | поведение как у in-memory storage gotd |
| T7 | Chat/ChannelForbidden и граничные случаи | ok-to-leave | `ChatToMap`/`ChannelToMap` отдают только `*tg.Chat`/`*tg.Channel` |
| T8 | суррогаты, `&` в JSON, typed-nil, пересекающиеся сущности | ok-to-leave | 300k случайных случаев без паник; `&` в JSONB раскодируется в `&`; паники ловит guard |
| T9 | ForumTopic без ReplyToMsgID; fallback отправителя | ok-to-leave | Telegram всегда ставит `reply_to_msg_id`; спорно, данные не теряются |
| T10 | `already_read` без `thread_id`, пропуски тестов, гонка Get/Set | ok-to-leave | гонка безвредна благодаря `GREATEST` |
| T11 | лог при superseded Fail; слабые тесты; нет валидации в `worker.New` | ok-to-leave | только текст лога; config валидирует |
| T12 | Background до Start; `Self()` на сообщение | ok-to-leave | gotd вызывает после старта; atomic load |
| T12 | ошибка `peers.Get` проглатывается | ok-to-leave, последствия → **F13** | |
| T13 | ключи `channel_id`/`chat_id`; двойные логи; двойной Self; healthcheck только IPv4 | ok-to-leave | косметика; `:8080` слушает и v4 |
| T13 | общий бюджет таймаута остановки | ok-to-leave, см. **F6** | |
| T13 | VERIFY `context.Canceled` → exit 1? | **закрыт** | нет (п. 2, 6/6b) |
| T14 | CHECK-канарейка только в Detail | **must-fix (низкий приоритет)** → **F7** | мутация доказала вакуумность |
| T14 | topic-title не проверяется, drain cap 20 | ok-to-leave | |
| T15 | generate-diff игнорирует untracked; кэш gha на fork-PR | ok-to-leave | сейчас один генерируемый файл; сборку не ломает |

**Rulings, которые оказались вредными:**
- **Ruling 13**: посылка о повторе неверна, см. **F2**.
- **Ruling 16**: в связке с отсутствием проверки сессии даёт подвисание. Само по себе приемлемо, исправляется через **F3**.

Остальные rulings (1–12, 14, 15, 17–19) претензий не вызывают.

---

## 5. Сильные стороны
- Защита от утечек сделана типами, а не на честном слове. `Secret` закрывает String, GoString, LogValue, JSON и Text. `Message`/`SaveMessage` реализуют `fmt.Formatter`. `SanitizeError` обходит всё дерево ошибок и вырезает значения из value-bearing ошибок pgx. Адаптер gotd пропускает только атрибуты из allowlist. В реальных логах compose при неверном пароле, хосте и DSN секретов нет.
- Очередь на чистом SQL: SKIP LOCKED, завершение с проверкой версии, NOTIFY в той же транзакции, UNLISTEN перед возвратом соединения в пул, свежий ctx для Complete/Fail.
- Упорядоченная остановка `runStages`: воркеры доделывают задание при живом Telegram, HTTP гаснет последним. Есть тесты порядка и таймаута.
- Тесты содержательные: настоящий `UpdateDispatcher` с моками API, схема на каждый тест, up/down/up миграций, FLOOD_WAIT, дубли и правки в e2e. Интеграционные тесты без `TEST_DATABASE_URL` падают, а не пропускаются.
- Образ distroless/nonroot с HEALTHCHECK. `migrate` требует только `DATABASE_URL`. CI проходит actionlint, все версии actions существуют, lint идёт с тегом integration.
