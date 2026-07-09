# API записи

Все ручки записи находятся под `/api/v1` и требуют Bearer-токена с областью `write`.

```
Authorization: Bearer <api-key>
```

Тенант определяется по ключу. В URL он не фигурирует.

---

## Ошибки аутентификации

| Статус | Значение |
|---|---|
| `401 Unauthorized` | Отсутствует или неверный API-ключ |
| `403 Forbidden` | Ключ не имеет нужной области или доступа к коллекции |

---

## Режим надёжности

Каждая ручка записи принимает необязательный заголовок `X-Letopis-Mode`, переопределяющий режим по умолчанию для данного запроса:

| Значение | Ответ | Гарантия |
|---|---|---|
| `strict` | `201` после записи в MongoDB | Синхронно; подтверждается до ответа |
| `durable` | `202` + тикет | Помещается в Redis Streams; воркер пишет асинхронно |
| `fast` | `202` + тикет | In-memory очередь; максимальная пропускная способность, ниже надёжность |

При отсутствии заголовка применяется `reliability_mode` коллекции (по умолчанию `durable`).

Ответы `202` возвращают `ticket_id`, который можно опрашивать через `GET /tickets/{id}`.

---

## Идемпотентность

Для предотвращения дублирования событий при повторе запроса передайте клиентски-сгенерированный ключ через:

- Заголовок `Idempotency-Key`, **или**
- Поле `event_id` в теле запроса (`event_id` имеет приоритет, если переданы оба).

Если тот же ключ поступает повторно в рамках окна дедупликации (по умолчанию 24 ч, настраивается), сервер воспроизводит оригинальный ответ:

- `200 {"status": "duplicate"}` — если оригинал уже записан.
- Тот же ответ `202` с тикетом — если оригинал принят, но ещё не сохранён.

Второе событие не записывается.

---

## Приём полного состояния

```
POST /api/v1/collections/{collection}/entities/{entityId}/state
```

Отправьте полное текущее состояние сущности. Сервер вычисляет дифф относительно последнего известного состояния. Если сущность новая, все поля фиксируются как операции `add`, а событие типируется как `create` (или `update`, если для коллекции настроен `first_event_op: update`).

**Параметры пути**

| Параметр | Описание |
|---|---|
| `collection` | Имя коллекции, например `crm.deals` (`^[a-z0-9]+(?:\.[a-z0-9]+)*$`) |
| `entityId` | Идентификатор сущности, любая непустая строка |

**Заголовки**

| Заголовок | Обязателен | Описание |
|---|---|---|
| `Authorization` | Да | `Bearer <key>` с областью `write` |
| `X-Letopis-Mode` | Нет | Переопределение режима надёжности |
| `Idempotency-Key` | Нет | Клиентский ключ дедупликации |

**Тело запроса**

```json
{
  "state": {
    "title": "ООО Акме",
    "amount": 5000,
    "stage": "prospect"
  },
  "op": "update",
  "event_id": "evt-001",
  "author_id": "user-42",
  "source": "crm-backend",
  "ts_source": "2026-06-01T10:00:00Z",
  "expected_version": 3,
  "meta": {
    "ip": "10.0.0.1",
    "session": "abc123"
  },
  "flow": {
    "flow_id": "flow-deal-onboarding",
    "step": "qualification",
    "caused_by": [
      {"activity_id": "act-111"}
    ]
  }
}
```

| Поле | Тип | Обязательно | Описание |
|---|---|---|---|
| `state` | object | Да | Полное текущее состояние сущности |
| `op` | string | Нет | `create` или `update`; выводится автоматически если отсутствует (см. `first_event_op`) |
| `event_id` | string | Нет | Клиентский ID для идемпотентности |
| `author_id` | string | Нет | Непрозрачный идентификатор автора изменения |
| `source` | string | Нет | Идентификатор системы-источника |
| `ts_source` | RFC3339 | Нет | Временная метка изменения в системе-источнике |
| `expected_version` | integer | Нет | Оптимистичная блокировка: `409` если текущая версия сущности отличается |
| `meta` | object | Нет | Произвольные метаданные, сохраняемые вместе с событием |
| `flow` | object | Нет | Блок бизнес-флоу (см. [Концепции — Бизнес-флоу](concepts.md)) |

**Ответы**

| Статус | Тело | Значение |
|---|---|---|
| `201` | `WriteResult` | Записано синхронно (режим `strict`) |
| `202` | `Accepted` | Принято асинхронно (`durable`/`fast`) |
| `200` | `{"status": "no_changes"}` | Состояние идентично последнему известному — событие не записано |
| `200` | `{"status": "duplicate"}` | Идемпотентное воспроизведение уже сохранённого события |
| `400` | `Error` | Некорректный запрос (неверное имя коллекции, отсутствует `state` и т.д.) |
| `409` | `Error` | `expected_version` не совпала |
| `413` | `Error` | Тело превышает `max_event_size_bytes` |
| `429` | `Error` | Очередь заполнена; повторите через `Retry-After` секунд |
| `503` | `Error` | Не удалось принять в очередь; повторите позже |

**WriteResult** (201):

```json
{
  "entity_id": "deal-1",
  "version": 4,
  "changes_count": 2
}
```

**Accepted** (202):

```json
{
  "ticket_id": "tkt_01J3...",
  "status": "accepted"
}
```

---

## Приём диффа

```
POST /api/v1/collections/{collection}/entities/{entityId}/diff
```

Отправьте готовый дифф. Полезно, когда система-источник уже вычислила, что изменилось. Сервер валидирует формат и сохраняет как есть.

**Тело запроса**

```json
{
  "changes": [
    {"path": "amount",  "op": "change", "old": 5000, "new": 7500},
    {"path": "stage",   "op": "change", "old": "prospect", "new": "qualified"}
  ],
  "op": "update",
  "author_id": "user-42",
  "source": "crm-backend",
  "ts_source": "2026-06-02T09:00:00Z"
}
```

| Поле | Тип | Обязательно | Описание |
|---|---|---|---|
| `changes` | array | Нет* | Список изменений полей |
| `state` | object | Нет* | Полное состояние (разрешено при `op: create` вместо `changes`) |
| `op` | string | Нет | `create`, `update` или `delete` |

\* Одно из `changes` или `state` должно присутствовать (за исключением `op: delete`, для которого используется ручка удаления).

**Объект изменения (change)**

| Поле | Тип | Описание |
|---|---|---|
| `path` | string | Путь к полю через точку, например `address.city` или `items.0.price` |
| `op` | string | `add`, `change` или `remove` |
| `old` | любой | Предыдущее значение (опускается для `add`) |
| `new` | любой | Новое значение (опускается для `remove`) |

Ответы идентичны ручке state.

---

## Фиксация удаления

```
POST /api/v1/collections/{collection}/entities/{entityId}/delete
```

Записывает сущность как удалённую. История сохраняется; возможна последующая «реинкарнация» новым `create`. Запись текущего состояния помечается `deleted: true`.

**Тело запроса** (необязательно):

```json
{
  "author_id": "user-42",
  "source": "crm-backend",
  "ts_source": "2026-06-03T12:00:00Z",
  "meta": {"reason": "объединена со сделкой deal-2"}
}
```

Все поля необязательны. Ответы — такие же, как у ручки state (без `413` и `409` по `expected_version`).

---

## Батч-приём

```
POST /api/v1/events:batch
```

Принимает до 1 000 событий по любым коллекциям и сущностям в одном запросе. Батч **не атомарный**: каждое событие валидируется независимо; невалидные элементы отклоняются и возвращаются в ответе; остальные публикуются как обычно.

Один заголовок `X-Letopis-Mode` применяется ко всем событиям в батче.

**Тело запроса**

```json
{
  "events": [
    {
      "collection": "crm.deals",
      "entity_id": "deal-1",
      "type": "state",
      "payload": {
        "state": {"amount": 8000},
        "author_id": "user-42"
      }
    },
    {
      "collection": "crm.contacts",
      "entity_id": "contact-99",
      "type": "diff",
      "payload": {
        "changes": [{"path": "email", "op": "change", "old": "old@x.com", "new": "new@x.com"}]
      }
    }
  ]
}
```

| Поле | Тип | Обязательно | Описание |
|---|---|---|---|
| `events` | array | Да | 1–1000 событий |
| `events[].collection` | string | Да | Целевая коллекция |
| `events[].entity_id` | string | Да | Целевая сущность |
| `events[].type` | string | Да | `state`, `diff` или `delete` |
| `events[].payload` | object | Да | Тело как у соответствующей per-event ручки |

**Ответ** (всегда `202`):

```json
{
  "ticket_id": "tkt_01J3...",
  "accepted": 2,
  "rejected": [
    {
      "index": 2,
      "error": {
        "code": "invalid_type",
        "message": "unknown event type: patch"
      }
    }
  ]
}
```

Статус `ticket_id` — `accepted`, если все события приняты, `partial` — если часть отклонена. Статус обработки per-event не отслеживается отдельно — тикет покрывает принятый батч как единицу.

**Лимиты**

| Лимит | Значение |
|---|---|
| Максимум событий в батче | 1 000 (возвращает `400` при превышении) |
| Максимальный размер тела | 32 МиБ (возвращает `413` при превышении) |

---

## Статус тикета

```
GET /api/v1/tickets/{ticketId}
```

Опросите статус асинхронной записи, принятой ответом `202`.

**Ответ** (200):

```json
{
  "ticket_id": "tkt_01J3...",
  "status": "stored",
  "entity_collection": "crm.deals",
  "entity_id": "deal-1",
  "created_at": "2026-06-01T10:00:00Z",
  "updated_at": "2026-06-01T10:00:01Z"
}
```

| `status` | Значение |
|---|---|
| `accepted` | Получено и помещено в очередь; воркер ещё не взял |
| `processing` | Воркер пишет в MongoDB |
| `stored` | Успешно записано |
| `failed` | Запись не удалась; причина в поле `error` |
| `partial` | Батч: часть событий сохранена, часть нет |

Тикеты истекают через настраиваемый TTL (по умолчанию 24 ч). Истёкший или неизвестный тикет возвращает `404`.

---

## Общий формат ошибок

Все ошибочные ответы используют единый конверт:

```json
{"error": "описание причины"}
```

Для отклонённых элементов батча используется более подробный объект:

```json
{
  "index": 2,
  "error": {
    "code": "too_large",
    "message": "payload exceeds max_event_size_bytes"
  }
}
```

---

## Примеры

### Синхронная запись с оптимистичной блокировкой

```sh
curl -X POST http://localhost:8080/api/v1/collections/crm.deals/entities/deal-1/state \
  -H "Authorization: Bearer $TOKEN" \
  -H "X-Letopis-Mode: strict" \
  -H "Content-Type: application/json" \
  -d '{
    "state": {"amount": 9000, "stage": "closed-won"},
    "expected_version": 7,
    "author_id": "user-42"
  }'
```

### Идемпотентный ingest диффа

```sh
curl -X POST http://localhost:8080/api/v1/collections/crm.deals/entities/deal-1/diff \
  -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: my-system-txn-id-abc" \
  -H "Content-Type: application/json" \
  -d '{
    "changes": [{"path": "amount", "op": "change", "old": 9000, "new": 10000}],
    "author_id": "user-42"
  }'
```

### Удаление и проверка тикета

```sh
# Удаляем
RESP=$(curl -s -X POST http://localhost:8080/api/v1/collections/crm.deals/entities/deal-1/delete \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"author_id":"admin"}' -H "Content-Type: application/json")
TICKET=$(echo $RESP | jq -r .ticket_id)

# Опрашиваем статус
curl http://localhost:8080/api/v1/tickets/$TICKET \
  -H "Authorization: Bearer $TOKEN"
```
