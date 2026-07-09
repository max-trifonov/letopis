# Admin API

Ручки администрирования требуют Bearer-токена с областью `admin`.

```
Authorization: Bearer <api-key>
```

Ключ с областью `admin` неявно получает также доступ `read`. Маска коллекций ключа по-прежнему применяется — admin-ключ с `collections: ["crm.*"]` не может управлять коллекциями `payments.*`.

---

## Конфигурация коллекции

### Получить конфигурацию коллекции

```
GET /api/v1/collections/{collection}/config
```

Возвращает эффективную конфигурацию коллекции (с применёнными дефолтами) и список полей, значения которых являются дефолтами, а не явно заданными.

Возвращает `404` для коллекции, у которой никогда не было явной конфигурации (автоматически созданные при первой записи коллекции не имеют записи конфига до явного `PUT`).

**Ответ** (200):

```json
{
  "config": {
    "reliability_mode": "durable",
    "snapshot_interval": 100,
    "retention": {"type": "forever"},
    "max_event_size_bytes": 1048576,
    "first_event_op": "create",
    "ordering": {"mode": "received"},
    "plugins": {
      "hash_chain": {
        "enabled": false,
        "fail_mode": "open",
        "params": {}
      }
    }
  },
  "defaults": ["reliability_mode", "snapshot_interval", "retention", "max_event_size_bytes", "first_event_op", "ordering"]
}
```

Массив `defaults` перечисляет каждое поле, значение которого является применённым дефолтом, а не явно сохранённым выбором. Используйте это, чтобы отличить «оператор выбрал durable» от «durable оказался дефолтом».

### Задать конфигурацию коллекции

```
PUT /api/v1/collections/{collection}/config
```

Сохраняет конфигурацию, создаёт физические коллекции и индексы MongoDB (идемпотентно), инвалидирует кэш резолвера (изменения вступают в силу немедленно) и записывает аудит-запись в `ev__system`.

Пропущенные поля возвращаются к дефолтам. Неизвестное значение enum или неположительное число возвращает `400`.

**Тело запроса**

```json
{
  "reliability_mode": "strict",
  "snapshot_interval": 50,
  "retention": {
    "type": "days",
    "days": 90
  },
  "max_event_size_bytes": 524288,
  "first_event_op": "update",
  "ordering": {
    "mode": "received"
  },
  "plugins": {
    "hash_chain": {
      "enabled": true,
      "fail_mode": "closed"
    }
  }
}
```

**Поля конфигурации**

| Поле | Тип | По умолчанию | Описание |
|---|---|---|---|
| `reliability_mode` | string | `durable` | Режим по умолчанию для запросов: `strict`, `durable` или `fast` |
| `snapshot_interval` | integer | 100 | Слепок каждые N событий; 0 — отключить слепки |
| `retention.type` | string | `forever` | Политика хранения: `forever`, `days` или `versions` |
| `retention.days` | integer | — | Обязательно при `type: days` |
| `retention.keep` | integer | — | Обязательно при `type: versions`; хранит N последних версий |
| `max_event_size_bytes` | integer | 1048576 | Лимит размера тела события (по умолчанию 1 МиБ) |
| `first_event_op` | string | `create` | Op для первого state-ingest новой сущности: `create` или `update` |
| `ordering.mode` | string | `received` | Порядок событий: `received` (по умолчанию) или `source` (зарезервировано, не реализовано) |
| `plugins` | object | `{}` | Конфиг плагинов, keyed по имени плагина |

**Конфиг плагина**

| Поле | Тип | По умолчанию | Описание |
|---|---|---|---|
| `enabled` | boolean | `false` | Включить плагин для этой коллекции |
| `fail_mode` | string | `open` | При ошибке плагина: `open` (писать без вклада плагина, логировать) или `closed` (отклонить запись) |
| `params` | object | `{}` | Параметры, специфичные для плагина |

**Ответ** (200):

```json
{
  "config": {
    "reliability_mode": "strict",
    "snapshot_interval": 50,
    ...
  }
}
```

---

## Правила

Правила вычисляются после каждой успешной записи и могут инициировать доставку вебхуков или запись в лог.

### Создать правило

```
POST /api/v1/collections/{collection}/rules
```

Валидирует условие (компиляцией) и все действия, затем сохраняет правило. Имя, уже используемое в коллекции, возвращает `409`. Коллекция берётся из пути.

**Тело запроса**

```json
{
  "name": "notify-on-close",
  "enabled": true,
  "condition": {
    "all": [
      {"field": "op", "eq": "update"},
      {"field": "changes", "match": {"path": "stage", "new": "closed-won"}}
    ]
  },
  "actions": [
    {
      "type": "webhook",
      "url": "https://hooks.example.com/crm-events",
      "secret_ref": "whsec_default",
      "timeout_ms": 3000,
      "retry": {
        "max_attempts": 3,
        "backoff": "exponential"
      }
    }
  ]
}
```

**Ответ** (201):

```json
{
  "rule": {
    "id": "rule_01J3...",
    "name": "notify-on-close",
    "enabled": true,
    "version": 1,
    "updated_at": "2026-06-01T10:00:00Z",
    "condition": {...},
    "actions": [...]
  }
}
```

### Список правил

```
GET /api/v1/collections/{collection}/rules
```

**Ответ** (200):

```json
{
  "rules": [
    {"id": "rule_01J3...", "name": "notify-on-close", "enabled": true, "version": 1, ...}
  ]
}
```

### Получить правило

```
GET /api/v1/collections/{collection}/rules/{ruleId}
```

Возвращает `404` для неизвестного ID правила.

### Обновить правило

```
PUT /api/v1/collections/{collection}/rules/{ruleId}
```

Заменяет тело правила и увеличивает его версию. Та же валидация, что при создании. Возвращает `404` для неизвестного правила, `409` при конфликте имён с другим правилом.

### Удалить правило

```
DELETE /api/v1/collections/{collection}/rules/{ruleId}
```

Возвращает `204 No Content` при успехе, `404` если правило не существует.

---

## Условия правил

Условие — дерево узлов. Каждый узел — одно из:

- **Комбинатор** — `all` (AND), `any` (OR) или `not`.
- **Скалярный лист** — `field` плюс ровно один оператор.
- **Лист матча изменений** — `field: "changes"` плюс объект `match`.

### Комбинаторы

```json
{"all": [<condition>, ...]}   // все должны быть истинны (пустой all = true)
{"any": [<condition>, ...]}   // хотя бы одно должно быть истинным (пустой any = false)
{"not": <condition>}          // отрицание
```

### Операторы скалярного листа

```json
{"field": "op", "eq": "update"}
{"field": "author_id", "ne": "system"}
{"field": "source", "in": ["crm-backend", "import-job"]}
{"field": "op", "exists": true}
```

| Оператор | Тип | Описание |
|---|---|---|
| `eq` | любой | Строгое равенство |
| `ne` | любой | Не равно |
| `in` | array | Значение входит в список |
| `gt` | number | Больше |
| `gte` | number | Больше или равно |
| `lt` | number | Меньше |
| `lte` | number | Меньше или равно |
| `regex` | string | Соответствие регулярному выражению (для строковых полей) |
| `exists` | boolean | Поле присутствует (`true`) или отсутствует (`false`) |

Адресуемые поля: `op`, `entity_id`, `author_id`, `source`.

### Лист матча изменений

Срабатывает, когда хотя бы один элемент в массиве `changes` события удовлетворяет всем указанным критериям. В `path` допустим glob, где `*` соответствует ровно одному сегменту dot-notation.

```json
{
  "field": "changes",
  "match": {
    "path": "items.*.price",
    "op": "change",
    "old": 100,
    "new": 200
  }
}
```

| Поле | Обязательно | Описание |
|---|---|---|
| `path` | Да | Glob-путь, например `status`, `address.city`, `items.*.price` |
| `op` | Нет | `add`, `change` или `remove` |
| `old` | Нет | Точное совпадение со старым значением изменения |
| `new` | Нет | Точное совпадение с новым значением изменения |

### Примеры условий

**Изменилось любое поле внутри `items`:**

```json
{"field": "changes", "match": {"path": "items.*"}}
```

**Статус изменился на «closed-won» конкретным автором:**

```json
{
  "all": [
    {"field": "author_id", "eq": "user-42"},
    {"field": "changes", "match": {"path": "status", "new": "closed-won"}}
  ]
}
```

**Событие удаления из любого источника, кроме «archive-job»:**

```json
{
  "all": [
    {"field": "op", "eq": "delete"},
    {"field": "source", "ne": "archive-job"}
  ]
}
```

---

## Действия правил

### Действие webhook

```json
{
  "type": "webhook",
  "url": "https://hooks.example.com/endpoint",
  "secret_ref": "whsec_default",
  "timeout_ms": 5000,
  "retry": {
    "max_attempts": 5,
    "backoff": "exponential"
  }
}
```

| Поле | Обязательно | Описание |
|---|---|---|
| `url` | Да | URL получателя (в продакшне обязательно HTTPS) |
| `secret_ref` | Нет | Ключ в `webhooks.secrets` конфига; при отсутствии запрос не подписывается |
| `timeout_ms` | Нет | Таймаут на попытку; дефолт — `webhooks.default_timeout_ms` |
| `retry.max_attempts` | Нет | Дефолт — `webhooks.max_attempts` |
| `retry.backoff` | Нет | `exponential` (единственный поддерживаемый вариант) |

**Формат запроса вебхука**

Сервер делает POST-запрос JSON на ваш endpoint:

```json
{
  "event": { ... },
  "rule": {"id": "rule_01J3...", "name": "notify-on-close"}
}
```

Заголовки каждого запроса вебхука:

| Заголовок | Описание |
|---|---|
| `X-HM-Signature` | `sha256=` + hex(HMAC-SHA256(secret, body)) |
| `X-HM-Delivery` | Стабильный ID доставки между повторами (для дедупликации) |
| `X-HM-Rule` | ID правила |

Проверка подписи на стороне получателя:

```python
import hmac, hashlib

def verify(body: bytes, header: str, secret: str) -> bool:
    expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, header)
```

Доставка — at-least-once. Ваш endpoint должен быть идемпотентным по `X-HM-Delivery`.

### Действие log

```json
{
  "type": "log",
  "level": "warn"
}
```

Записывает структурированную лог-запись с событием и ID правила. `level` — произвольная строка; сервис логирует её как есть.

---

## Dead-letter queue (DLQ)

Когда вебхук исчерпывает все попытки повтора, он попадает в DLQ правила.

### Список записей DLQ

```
GET /api/v1/collections/{collection}/rules/{ruleId}/dlq
```

**Параметры запроса**

| Параметр | По умолчанию | Описание |
|---|---|---|
| `limit` | 100 | Размер страницы (максимум 1 000) |
| `cursor` | — | Непрозрачный токен пагинации |

**Ответ** (200):

```json
{
  "items": [
    {
      "id": "dlq_01J3...",
      "rule_id": "rule_01J3...",
      "collection": "crm.deals",
      "delivery_id": "dlv_01J3...",
      "url": "https://hooks.example.com/crm-events",
      "secret_ref": "whsec_default",
      "attempts": 5,
      "last_error": "timeout",
      "failed_at": "2026-06-02T10:00:00Z",
      "body": { ... }
    }
  ],
  "next_cursor": null
}
```

`body` — точный JSON-payload, который был (или должен был быть) отправлен. `delivery_id` — стабильный ID, передаваемый в заголовке `X-HM-Delivery` — используйте его для дедупликации на стороне получателя после повторной отправки.

### Повторная отправка записей DLQ

```
POST /api/v1/collections/{collection}/rules/{ruleId}/dlq:redeliver
```

Повторно помещает записи DLQ в очередь для ещё одной попытки доставки и удаляет их из DLQ при успешном повторном добавлении.

**Тело запроса** (необязательно):

```json
{
  "ids": ["dlq_01J3...", "dlq_01J4..."]
}
```

Не передавайте `ids` (или отправьте пустое тело), чтобы повторить отправку всех записей DLQ данного правила.

**Ответ** (202):

```json
{"requeued": 3}
```

Повторно отправленные доставки подчиняются той же политике ретраев, что и оригинальные. Если они снова не удадутся — вернутся в DLQ. Повторная доставка использует тот же `delivery_id`.

Возвращает `404`, если какой-либо из указанных ID неизвестен или уже повторно отправлен.

---

## Аудит-лог

Все административные действия записываются в коллекцию `ev__system` тенанта:

- `collection.config.updated` — при успешном `PUT /collections/{c}/config`
- `rule.created`, `rule.updated`, `rule.deleted` — при CRUD правил

Аудит-записи хранятся с ID тенанта в качестве актора и доступны через прямые запросы к MongoDB (API для аудит-лога в текущей версии отсутствует).
