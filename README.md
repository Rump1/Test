# Async Payment Processing Service

Асинхронный микросервис процессинга платежей для тестового задания.

Сервис принимает платеж через HTTP API, сохраняет его в PostgreSQL, гарантированно публикует событие через outbox, обрабатывает платеж в RabbitMQ consumer-е, эмулирует внешний платежный шлюз и уведомляет клиента о результате через webhook.

## Возможности

- Создание платежа: `POST /api/v1/payments`
- Получение платежа: `GET /api/v1/payments/{payment_id}`
- Статический API key в заголовке `X-API-Key`
- Идемпотентность через обязательный заголовок `Idempotency-Key`
- Защита от повторного использования idempotency key с другим payload: `409 Conflict`
- Outbox pattern для надежной публикации `payment.created`
- RabbitMQ queue `payments.new`
- Retry queue `payments.new.retry`
- Dead Letter Queue `payments.new.dlq`
- Эмуляция платежного шлюза: задержка 2-5 секунд, 90% success, 10% failure
- Webhook notification с 3 попытками доставки
- Alembic migration для таблиц `payments` и `outbox`
- Docker Compose окружение с PostgreSQL, RabbitMQ, API, outbox publisher и consumer
- Pytest coverage для ключевой бизнес-логики

## Технологии

- Python 3.12
- FastAPI
- Pydantic v2
- SQLAlchemy 2.0 async
- PostgreSQL
- RabbitMQ
- FastStream
- Alembic
- Docker Compose
- pytest
- uv

## Архитектура

Код разделён на четыре слоя с зависимостями, направленными внутрь:

```text
app/
  domain/          # Payment, PaymentRequest, результаты оплаты/доставки, OutboxEvent
  application/     # Payments, ProcessPayment, WebhookDelivery, PublishOutbox, ports
  infrastructure/  # SQLAlchemy repositories + UnitOfWork, RabbitMQ, httpx, шлюз
  presentation/    # HTTP-схемы/маршруты и разбор входящих сообщений
  bootstrap.py     # сборка конкретных зависимостей
  main.py          # HTTP entrypoint и lifespan
  workers/         # consumer и outbox entrypoints
```

`domain` зависит только от стандартной библиотеки. `application` зависит от
`domain` и собственных контрактов в `ports.py`. Инфраструктурные adapters
реализуют эти контракты; входные adapters вызывают прикладные сценарии.
Проверка `tests/test_architecture.py` запрещает обратные импорты.

- `Payment` — неизменяемый доменный объект. `finalize` и `record_delivery`
  проверяют допустимость перехода и возвращают новое состояние.
- `PaymentRequest` проверяет данные без HTTP/Pydantic и защищает метаданные
  от изменения после вычисления отпечатка идемпотентности.
- `Payments` владеет атомарным созданием payment + outbox и обработкой конфликта
  ключа. HTTP adapter не получает сессию БД и не делает commit.
- `ProcessPayment` сохраняет результат оплаты, затем передаёт идентификатор
  платежа в `DeliverPaymentNotification`.
- `DeliverPaymentNotification` владеет блокировкой доставки, проверкой её статуса,
  сохранением результата и возобновлением публикации DLQ после сбоя.
- `WebhookDelivery` внутри module уведомлений возвращает `DeliveryResult`,
  не меняя платёж. HTTP adapter выполняет одну попытку; module определяет retry
  и сохраняет фактическое число попыток даже при неожиданной ошибке transport.
- `PublishOutbox` фиксирует каждую попытку отдельно; ожидание retry проходит
  вне транзакции. Конкурентное владение событием обеспечивает SQL adapter.
- `RabbitEventPublisher` скрывает имена очередей, routing, persistence и TTL.
  Подтверждение публикации обязательно; ошибка публикации не превращается в ACK.
- Конкретные зависимости передаются конструкторам. Ресурсы создаются в точках
  запуска и закрываются через lifespan/finally, включая ошибки запуска consumer.

ООП используется для инкапсуляции состояния, композиции зависимостей и
полиморфизма adapters через `Protocol`. Универсального CRUD-репозитория и
иерархии базовых сценариев нет: контракты соответствуют действиям с платежами.

Тесты прикладных сценариев выполняются с двумя adapters хранения: в памяти
и SQLite. PostgreSQL smoke-проверка подтверждает конкурентную обработку и
фиксацию результата до HTTP; SQLite не используется как доказательство
корректности PostgreSQL-блокировок. Контракт HTTP и схема БД сохранены,
дополнительная миграция не требуется.

Термины предметной области описаны в [CONTEXT.md](CONTEXT.md).

В `docker-compose.yml` описаны 5 сервисов:

- `postgres`: база данных.
- `rabbitmq`: брокер сообщений и RabbitMQ Management UI.
- `api`: FastAPI приложение.
- `outbox-publisher`: отдельный процесс публикации outbox событий.
- `consumer`: FastStream consumer платежных сообщений.

Основной flow:

1. Клиент вызывает `POST /api/v1/payments`.
2. Прикладной сценарий в одной DB-транзакции создает:
   - запись в `payments` со статусом `pending`;
   - запись в `outbox` с событием `payment.created`.
3. `outbox-publisher` читает pending outbox events и публикует событие в RabbitMQ queue `payments.new`.
4. `consumer` получает сообщение из `payments.new`.
5. Consumer эмулирует внешний платежный шлюз.
6. Consumer фиксирует финальный платежный статус:
   - `succeeded`, если gateway успешен;
   - `failed`, если gateway вернул отказ.
7. После commit финального статуса consumer отправляет webhook.
8. Ошибка webhook не меняет статус платежа.

Outbox используется для события `payment.created -> payments.new`. Webhook доставка выполняется внутри одного consumer-а, потому что по заданию consumer должен делать весь processing flow.

## Модель платежа

Платеж содержит:

- `id`: UUID
- `amount`: decimal, `NUMERIC(12, 2)`, больше нуля
- `currency`: `RUB`, `USD`, `EUR`
- `description`: 1-500 символов
- `metadata`: JSON object до 16 KB
- `status`: `pending`, `succeeded`, `failed`
- `idempotency_key`: уникальный ключ запроса
- `request_fingerprint`: hash нормализованного payload
- `webhook_url`: HTTP/HTTPS URL
- `webhook_status`: `pending`, `sent`, `failed`
- `webhook_attempts`
- `webhook_last_error`
- `webhook_sent_at`
- `created_at`
- `processed_at`
- `failure_reason`

`payment.status = failed` означает только отказ платежного шлюза. Это не означает проблему webhook, RabbitMQ или инфраструктуры.

## Запуск

```bash
docker compose up --build
```

API:

```text
http://localhost:8000
```

RabbitMQ Management UI:

```text
http://localhost:15672
login: guest
password: guest
```

Миграции применяются при старте контейнера `api`:

```bash
alembic upgrade head
```

## Переменные окружения

| Name | Default |
| --- | --- |
| `API_KEY` | `dev-api-key` |
| `DATABASE_URL` | `postgresql+asyncpg://payments:payments@postgres:5432/payments` |
| `RABBITMQ_URL` | `amqp://guest:guest@rabbitmq:5672/` |
| `OUTBOX_POLL_INTERVAL_SECONDS` | `1` |

## API

Все endpoints требуют заголовок:

```text
X-API-Key: dev-api-key
```

### Создать платеж

```http
POST /api/v1/payments
```

Обязательные headers:

```text
X-API-Key: dev-api-key
Idempotency-Key: order-123
Content-Type: application/json
```

Пример:

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: dev-api-key' \
  -H 'Idempotency-Key: order-123' \
  -d '{
    "amount": "1500.50",
    "currency": "RUB",
    "description": "Order #123",
    "metadata": {"order_id": "123"},
    "webhook_url": "https://webhook.site/your-url"
  }'
```

Успешный ответ:

```http
202 Accepted
```

```json
{
  "payment_id": "07aa9e5a-af6b-44c5-857e-e8f08b5dcf3f",
  "status": "pending",
  "created_at": "2026-07-08T09:42:04.139670Z"
}
```

### Получить платеж

```http
GET /api/v1/payments/{payment_id}
```

Пример:

```bash
curl -i http://localhost:8000/api/v1/payments/07aa9e5a-af6b-44c5-857e-e8f08b5dcf3f \
  -H 'X-API-Key: dev-api-key'
```

Ответ содержит полную информацию о платеже, включая платежный статус и статус webhook доставки.

## Ошибки API

- `400 Bad Request`: отсутствует `Idempotency-Key`.
- `401 Unauthorized`: отсутствует или неверный `X-API-Key`.
- `404 Not Found`: платеж не найден.
- `409 Conflict`: тот же `Idempotency-Key`, но другой request payload.
- `422 Unprocessable Entity`: ошибка валидации body или path параметров.

## Идемпотентность

`Idempotency-Key` обязателен для создания платежа.

При первом запросе сервис сохраняет fingerprint нормализованного payload:

- `amount`
- `currency`
- `description`
- `metadata`
- `webhook_url`

Поведение повторных запросов:

- тот же key и тот же payload -> вернуть существующий платеж с `202 Accepted`;
- тот же key и другой payload -> вернуть `409 Conflict`.

Ключ уникален глобально для сервиса.

Сервис также обрабатывает race condition: если два одинаковых запроса с одним `Idempotency-Key` приходят одновременно, unique constraint в PostgreSQL не приводит к дублю платежа.

## Outbox

API не публикует события в RabbitMQ напрямую.

При создании платежа в одной транзакции создаются:

- `payments`;
- `outbox(event_type = payment.created)`.

`outbox-publisher` получает ограниченный снимок pending событий через
`FOR UPDATE SKIP LOCKED` и сразу освобождает блокировки снимка. Снимок не является
резервированием: перед каждой попыткой `claim` заново проверяет состояние одного
события и блокирует его через `FOR UPDATE SKIP LOCKED`. Занятые другим publisher
и уже опубликованные события пропускаются.

Публикация и сохранение её результата выполняются в отдельной транзакции для
каждой попытки. После подтверждения брокера событие получает `status = published`;
после ошибки фиксируются счётчик попыток и последняя ошибка. Паузы retry проходят
после освобождения транзакции. Размер пачки ограничивает число выбранных событий,
но больше не определяет время удержания одной транзакции.

Сбой в конце пачки не откатывает ранее зафиксированные публикации. Если брокер
принял сообщение, а commit этой попытки не состоялся, возможна повторная публикация
этого события: гарантия остаётся at-least-once. Блокировка текущего события остаётся
на время сетевой публикации; lease и дополнительные состояния в БД не используются.

Сообщения persistent, очереди durable; publisher подтверждает маршрутизацию
и предварительно создаёт очереди. RabbitMQ хранит данные в Docker volume.
Workers ждут запуска HTTP module после миграций; publisher продолжает опрос
после временных ошибок БД.

Если публикация в RabbitMQ падает:

- до 3 попыток на событие за один цикл publisher; другой publisher может
  продолжить событие во время backoff, поэтому это не глобальный лимит;
- задержки между тремя попытками: `1s`, `2s`;
- после исчерпания попыток событие остаётся `pending` и повторяется в следующем цикле;
- ранее сохранённые `failed`-события также подбираются автоматически;
- RabbitMQ DLQ не используется, потому что сообщение еще не было опубликовано в RabbitMQ.

## RabbitMQ Retry и DLQ

Очереди:

- `payments.new`: основная очередь обработки платежей.
- `payments.new.retry`: retry queue.
- `payments.new.dlq`: Dead Letter Queue.

Если consumer не смог обработать payment message до финального статуса платежа:

1. Сообщение публикуется в `payments.new.retry`.
2. Для retry сообщения задается per-message TTL через `expiration`.
3. Retry queue dead-letter-ит сообщение обратно в `payments.new`.
4. После третьей неудачной попытки сообщение публикуется в `payments.new.dlq`.

Worker не блокируется через `asyncio.sleep` во время payment retry.

Если webhook не доставлен после трёх попыток, в DLQ публикуется сообщение
со `stage = webhook`, `payment_id`, числом попыток и последней ошибкой.
Статус платежа при этом не меняется. При ошибке публикации в retry/DLQ исходное
сообщение возвращается в очередь (NACK с requeue).

## Webhook

Webhook отправляется после того, как финальный платежный статус уже записан в БД.

HTTP contract:

- method: `POST`
- content type: `application/json`
- timeout: `3s`
- success: любой HTTP `2xx`
- failure: network error, timeout или любой non-2xx
- 3 попытки доставки

Headers:

```text
Content-Type: application/json
X-Payment-Event: payment.succeeded
X-Payment-Id: <payment_id>
```

Payload:

```json
{
  "event_id": "uuid",
  "event_type": "payment.succeeded",
  "payment_id": "07aa9e5a-af6b-44c5-857e-e8f08b5dcf3f",
  "status": "succeeded",
  "amount": "1500.50",
  "currency": "RUB",
  "processed_at": "2026-07-08T09:42:08.383005Z",
  "metadata": {"order_id": "123"}
}
```

Если webhook не доставлен после 3 попыток:

- `webhook_status = failed`;
- `webhook_attempts = 3`;
- `webhook_last_error` содержит последнюю ошибку;
- `payment.status` не меняется;
- сообщение о неудачной доставке публикуется в `payments.new.dlq`.

Неожиданная ошибка transport завершает доставку с фактическим числом выполненных
попыток. Ошибки orchestration/БД и отмена обработки не подменяются вымышленным
результатом доставки: они распространяются до входного adapter для повторной
обработки. Ошибка публикации DLQ после commit не запускает HTTP-доставку заново.

При повторной доставке сообщения consumer возобновляет pending webhook уже
завершённого платежа, не вызывая шлюз повторно. Доставленный webhook пропускается.
При сбое после HTTP-запроса, но до commit возможна повторная отправка: клиенту
нужно устранять дубли по стабильному `event_id`. Лимит трёх HTTP-попыток действует
на один запуск доставки; аварийный перезапуск до commit может повторить этот цикл.
Сообщения DLQ также могут дублироваться при сбое между публикацией и ACK.

## Локальная разработка

Установить зависимости:

```bash
uv sync
```

Запустить тесты:

```bash
uv run pytest -v
```

Запустить lint:

```bash
uv run ruff check .
```

Проверить Alembic SQL:

```bash
uv run alembic upgrade head --sql
```

## Проверка Docker окружения

После запуска:

```bash
docker compose ps
```

Проверить очереди:

```bash
docker compose exec -T rabbitmq rabbitmqctl list_queues name messages messages_ready messages_unacknowledged
```

Проверить последние платежи:

```bash
docker compose exec -T postgres psql -U payments -d payments \
  -c "select status, webhook_status, webhook_attempts, webhook_last_error from payments order by created_at desc limit 5;"
```

Остановить окружение:

```bash
docker compose down
```

Удалить volume с PostgreSQL данными:

```bash
docker compose down -v
```

## Автоматическая проверка Docker

После `docker compose up --build -d`:

```bash
docker compose exec -T api python - < scripts/smoke_docker.py
docker compose exec -T api python - < scripts/check_postgres.py
```

Проверяет успешный webhook, три HTTP-ошибки и DLQ, задержки RabbitMQ retry
1 и 2 секунды, идемпотентность API и параллельные дубли на PostgreSQL.
Smoke-проверка создаёт два тестовых платежа; чужие сообщения DLQ не удаляет.
`check_postgres.py` проверяет конкурентное создание одного платежа и outbox,
конфликт изменённого запроса и `SKIP LOCKED` в отдельной временной схеме PostgreSQL;
схема удаляется после проверки.

### Регрессионные проверки PostgreSQL через pytest

Обычный запуск использует memory и SQLite. Чтобы добавить PostgreSQL ко всем
проверкам общего UoW interface и выполнить конкурентные сценарии:

```bash
TEST_POSTGRES_URL=postgresql+asyncpg://payments:payments@localhost:5432/payments \
  uv run pytest -q
```

Каждый PostgreSQL-тест создаёт отдельную временную схему и удаляет её после
завершения. Проверяются независимые publisher, пропуск заблокированного события,
освобождение транзакции на backoff, сбой после частичной публикации и конкурентная
доставка уведомления. Без `TEST_POSTGRES_URL` специфичные PostgreSQL-тесты пропускаются.

## Проверенное состояние

Проект проверялся командами:

```bash
uv run pytest -v
uv run ruff check .
uv run python -m compileall app alembic
uv run alembic upgrade head --sql
docker compose up --build -d
```

Docker end-to-end проверка подтвердила:

- API создает платеж с `202 Accepted`;
- повтор с тем же `Idempotency-Key` возвращает тот же платеж;
- повтор с другим payload возвращает `409 Conflict`;
- outbox событие публикуется;
- consumer обрабатывает сообщение;
- платеж переходит в финальный статус;
- webhook выполняет retry и сохраняет результат доставки.
