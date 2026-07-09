# API чтения

Все ручки чтения находятся под `/api/v1` и требуют Bearer-токена с областью не ниже `read`.

```
Authorization: Bearer <api-key>
```

---

## История сущности

```
GET /api/v1/collections/{collection}/entities/{entityId}/history
```

Возвращает страницу событий сущности, по умолчанию от новых к старым.

**Параметры запроса**

| Параметр | Тип | По умолчанию | Описание |
|---|---|---|---|
| `from` | RFC3339 | — | События, полученные не ранее этого времени |
| `to` | RFC3339 | — | События, полученные не позднее этого времени |
| `author_id` | string | — | Фильтр по автору |
| `op` | string | — | Фильтр по операции: `create`, `update` или `delete` |
| `path` | string | — | События, затрагивающие этот путь поля или вложенный под ним |
| `source` | string | — | Фильтр по идентификатору системы-источника |
| `limit` | integer | 100 | Размер страницы (1–1 000) |
| `cursor` | string | — | Непрозрачный токен пагинации из предыдущего `next_cursor` |
| `order_by` | string | `version` | Ключ сортировки: `version`, `ts_source` или `ts_received` |
| `order` | string | `desc` | `asc` или `desc` |
| `format` | string | `native` | Формат диффа: `native` (формат Letopis) или `json-patch` (RFC 6902) |

**Ответ** (200):

```json
{
  "entity_id": "deal-1",
  "next_cursor": "eyJ2IjoxMH0=",
  "events": [
    {
      "version": 4,
      "op": "update",
      "ts_received": "2026-06-02T09:00:01Z",
      "ts_stored":   "2026-06-02T09:00:01Z",
      "ts_source":   "2026-06-02T09:00:00Z",
      "author_id": "user-42",
      "source": "crm-backend",
      "meta": {},
      "integrity": {
        "hash":      "sha256:9f86d0...",
        "prev_hash": "sha256:2c2640..."
      },
      "changes": [
        {"path": "amount", "op": "change", "old": 5000, "new": 7500},
        {"path": "stage",  "op": "change", "old": "prospect", "new": "qualified"}
      ]
    }
  ]
}
```

Поле `integrity` присутствует только когда для коллекции включён плагин `hash_chain`.

При `format=json-patch` каждый элемент `changes` следует RFC 6902: `{"op": "replace", "path": "/amount", "value": 7500}`. Нативный формат богаче (содержит `old`) и предпочтителен для отображения.

**Пагинация:** Передавайте значение `next_cursor` из предыдущего ответа как параметр `cursor`. Когда `next_cursor` равен `null`, достигнута последняя страница.

---

## Текущее состояние

```
GET /api/v1/collections/{collection}/entities/{entityId}/state
```

Возвращает текущее материализованное состояние сущности.

**Ответ** (200):

```json
{
  "entity_id": "deal-1",
  "version": 4,
  "ts": "2026-06-02T09:00:01Z",
  "deleted": false,
  "state": {
    "title": "ООО Акме",
    "amount": 7500,
    "stage": "qualified"
  }
}
```

Возвращает `404`, если сущность ни разу не записывалась (или была удалена через purge).

---

## Состояние на момент времени (point-in-time)

```
GET /api/v1/collections/{collection}/entities/{entityId}/state?version=N
GET /api/v1/collections/{collection}/entities/{entityId}/state?at=<RFC3339>
```

Восстанавливает состояние сущности на прошлую версию или временной срез по времени получения. Параметры `version` и `at` взаимоисключающие.

| Параметр | Описание |
|---|---|
| `version` | Восстановить на эту версию (обрезается до последней, если больше) |
| `at` | Восстановить по этому временному срезу `ts_received` (RFC3339); возвращает состояние после последнего события с `ts_received ≤ at` |

`?at` всегда использует `ts_received` (метку времени в порядке записи, ADR-011). Вариант `?at_source` зарезервирован для будущего режима упорядочивания и возвращает `400`.

**Ответ** (200) добавляет `reconstructed_from`, чтобы отличить его от ответа текущего состояния:

```json
{
  "entity_id": "deal-1",
  "version": 2,
  "ts": "2026-06-01T10:00:01Z",
  "deleted": false,
  "state": {
    "title": "ООО Акме",
    "amount": 5000,
    "stage": "prospect"
  },
  "reconstructed_from": {
    "snapshot_version": null,
    "events_applied": 2
  }
}
```

`snapshot_version` — версия слепка, использованного как основа, или `null`, если реконструкция началась с генезиса (слепки недоступны). `events_applied` — количество хвостовых событий, воспроизведённых поверх основы.

Возвращает `404`, если сущность не существует или `?at` раньше первого события сущности.  
Возвращает `400`, если переданы оба параметра `version` и `at`, `version < 1` или используется `at_source`.

---

## Список коллекций

```
GET /api/v1/collections
```

Возвращает все коллекции, доступные API-ключу (отфильтрованные по маске коллекций ключа), с базовой статистикой.

**Ответ** (200):

```json
{
  "collections": [
    {
      "name": "crm.deals",
      "entities": 1243,
      "events": 18750,
      "last_event_at": "2026-06-02T09:00:01Z",
      "config": {
        "reliability_mode": "durable",
        "snapshot_interval": 100,
        "retention": {"type": "forever"},
        "max_event_size_bytes": 1048576,
        "first_event_op": "create",
        "ordering": {"mode": "received"}
      }
    }
  ]
}
```

`entities` — точное число различных сущностей (из коллекции `cur_*`). `events` — приблизительное число событий (MongoDB `estimatedDocumentCount`, быстро и неблокирующе). `last_event_at` — сохранённая метка времени самого нового события, или `null` для коллекции без событий.

Автоматически созданные коллекции (записанные без явного `PUT /config`) включаются. В `config` применены дефолты.

---

## Бизнес-флоу

### Запись активности

```
POST /api/v1/activities
```

Записывает бизнес-процессное событие (не изменение сущности) и привязывает его к флоу. Требует области `write`.

**Тело запроса**

```json
{
  "type": "invoice.approval.started",
  "flow_id": "flow-invoice-cycle-42",
  "author_id": "user-7",
  "source": "approval-service",
  "ts_source": "2026-06-02T11:00:00Z",
  "caused_by": [
    {"collection": "invoices", "entity_id": "inv-99", "version": 3}
  ],
  "refs": [
    {"collection": "crm.deals", "entity_id": "deal-1"}
  ],
  "data": {
    "approver_email": "manager@example.com"
  },
  "meta": {}
}
```

| Поле | Описание |
|---|---|
| `activity_id` | Необязательно; сервер генерирует ULID при отсутствии |
| `type` | Семантический ярлык (непрозрачен для Letopis) |
| `flow_id` | Флоу для привязки; создаётся новый, если отсутствует |
| `caused_by` | Вышестоящие события или активности, ставшие причиной текущей |
| `refs` | Связанные сущности (информационно, не причинно) |
| `data` | Произвольный payload |

**Ответ** (201):

```json
{
  "activity_id": "act_01J3...",
  "flow_id": "flow-invoice-cycle-42"
}
```

### Получение флоу

```
GET /api/v1/flows/{flowId}
```

Возвращает все узлы флоу (события и активности) в порядке получения с их причинными рёбрами.

**Параметры запроса**

| Параметр | По умолчанию | Описание |
|---|---|---|
| `limit` | 100 | Размер страницы (1–1 000) |
| `cursor` | — | Непрозрачный токен пагинации |

**Ответ** (200):

```json
{
  "flow_id": "flow-invoice-cycle-42",
  "next_cursor": null,
  "nodes": [
    {
      "kind": "event",
      "ts_received": "2026-06-01T10:00:01Z",
      "collection": "invoices",
      "entity_id": "inv-99",
      "version": 3,
      "op": "update",
      "step": "amount-adjusted",
      "caused_by": []
    },
    {
      "kind": "activity",
      "ts_received": "2026-06-02T11:00:01Z",
      "activity_id": "act_01J3...",
      "type": "invoice.approval.started",
      "caused_by": [
        {"collection": "invoices", "entity_id": "inv-99", "version": 3}
      ],
      "refs": [
        {"collection": "crm.deals", "entity_id": "deal-1"}
      ],
      "data": {"approver_email": "manager@example.com"}
    }
  ]
}
```

Каждый узел имеет поле `kind: event | activity`. Узлы-события включают `collection`, `entity_id`, `version`, `op` и `step`. Узлы-активности включают `activity_id`, `type`, `refs` и `data`. Оба типа содержат `caused_by` и `ts_received`.

---

## Примеры

### Получить последние 10 изменений поля

```sh
curl "http://localhost:8080/api/v1/collections/crm.deals/entities/deal-1/history?path=stage&limit=10" \
  -H "Authorization: Bearer $TOKEN"
```

### Восстановить состояние неделю назад

```sh
curl "http://localhost:8080/api/v1/collections/crm.deals/entities/deal-1/state?at=2026-05-26T00:00:00Z" \
  -H "Authorization: Bearer $TOKEN"
```

### Постраничный перебор истории

```sh
# Первая страница
RESP=$(curl -s "http://localhost:8080/api/v1/collections/crm.deals/entities/deal-1/history?limit=50" \
  -H "Authorization: Bearer $TOKEN")
CURSOR=$(echo $RESP | jq -r .next_cursor)

# Следующая страница
curl "http://localhost:8080/api/v1/collections/crm.deals/entities/deal-1/history?limit=50&cursor=$CURSOR" \
  -H "Authorization: Bearer $TOKEN"
```
