# Начало работы

Это руководство охватывает установку, настройку и запуск первого экземпляра Letopis.

## Требования

| Зависимость | Минимальная версия | Примечание |
|---|---|---|
| Go | 1.25 | Нужен для сборки из исходников |
| MongoDB | 7.0 | Отдельная база данных на каждого тенанта создаётся автоматически |
| Redis | 6.0 | Используется для durable-очереди, ключей идемпотентности и инвалидации кэша правил |
| Docker | любая | Опционально; нужен для compose-стеков и интеграционных тестов |

---

## Установка

### Готовые бинари

Скачайте архив под свою платформу (linux/darwin/windows, amd64/arm64) со страницы [GitHub Releases](https://github.com/max-trifonov/letopis/releases). В архиве — бинарь `letopis`, `LICENSE`, `NOTICE`, `config.example.yaml` и `docker-compose.deps.yml`.

### Из исходников

```sh
git clone https://github.com/max-trifonov/letopis.git
cd letopis
make build          # создаёт bin/letopis
```

Бинарь встраивает версию, хэш коммита и дату сборки через `ldflags`; `make build` берёт их из `git describe` автоматически.

---

## Docker

Образ публикуется в `ghcr.io/max-trifonov/letopis`. Для локальной сборки:

```sh
make docker          # тегирует ghcr.io/max-trifonov/letopis:latest
```

Доступны два compose-варианта:

### Dev-стек

```sh
docker compose -f docker-compose.dev.yml up --build
```

Собирает образ из рабочей директории и запускает MongoDB, Redis и сервис вместе. Переменные окружения и `.env` переопределяют любой параметр `LETOPIS_*`.

### Production-стек

```sh
cp config.example.yaml config.yaml   # отредактируйте перед запуском
docker compose up -d
```

Использует опубликованный образ. MongoDB и Redis включены в compose-файл для удобства; в продакшне обычно `mongodb.uri` и `redis.addr` указывают на управляемую инфраструктуру.

---

## Конфигурация

Letopis настраивается через YAML-файл. Бинарь ищет `config.yaml` в следующем порядке:

1. Путь, указанный флагом `--config` (наивысший приоритет)
2. Текущая рабочая директория
3. Директория рядом с бинарём

**Конфигурационный файл обязателен — без него сервер не стартует.**

После загрузки файла любая переменная окружения `LETOPIS_*` переопределяет соответствующий ключ:

| Переменная окружения | Ключ конфига |
|---|---|
| `LETOPIS_ROLE` | `role` |
| `LETOPIS_HTTP_ADDR` | `server.http.addr` |
| `LETOPIS_GRPC_ADDR` | `server.grpc.addr` |
| `LETOPIS_MONGODB_URI` | `mongodb.uri` |
| `LETOPIS_REDIS_ADDR` | `redis.addr` |
| `LETOPIS_REDIS_PASSWORD` | `redis.password` |
| `LETOPIS_REDIS_DB` | `redis.db` |
| `LETOPIS_LOG_LEVEL` | `log.level` |

### Полный справочник по конфигурации

```yaml
# Роль: api (только HTTP/gRPC), worker (только пайплайн), all (оба — по умолчанию).
role: all

server:
  http:
    addr: ":8080"
    tls:
      enabled: false
      cert_file: ""   # путь до PEM-сертификата
      key_file: ""    # путь до PEM-ключа
  grpc:
    addr: ":9090"
    tls:
      enabled: false
      cert_file: ""
      key_file: ""

# Кластер MongoDB по умолчанию. Тенанты могут переопределить его индивидуально.
mongodb:
  uri: "mongodb://localhost:27017"

redis:
  addr: "localhost:6379"
  password: ""
  db: 0

# Очередь событий (ADR-003).
# Драйвер memory: только in-process, валиден только для role=all.
# redis-streams: по умолчанию, работает при разделении api/worker-процессов.
# Изменение числа шардов на живой очереди требует её предварительного опустошения.
queue:
  driver: redis-streams        # memory | redis-streams
  shards: 16
  stream_prefix: "letopis:ingest"
  consumer_group: "workers"

# Движок правил — скомпилированные правила кэшируются per-collection.
# Изменение правила рассылается через Redis pub/sub и тут же инвалидируется.
# cache_ttl_seconds ограничивает устаревание кэша при недоступности pub/sub.
rules:
  cache_ttl_seconds: 30

# Доставка вебхуков.
webhooks:
  default_timeout_ms: 5000
  max_attempts: 5
  backoff:
    base_ms: 500
    max_ms: 30000
  delivery_shards: 0           # 0 = то же число, что у queue.shards
  secrets:
    whsec_default: "change-me-in-production"
  ssrf:
    allow_private: false       # true только если получатели в вашей приватной сети
    allow_http: false          # true только для разработки; в продакшне требуется https

log:
  level: info    # debug | info | warn | error
  format: json   # json | text

collections:
  auto_create: true   # false — требовать явного PUT /config перед первой записью

tenants:
  - id: acme
    database:
      uri: ""    # опционально: указать другой кластер
      name: ""   # опционально: явное имя базы данных (по умолчанию: hm_t_acme)
    keys:
      - key_hash: "sha256:<hex>"   # предпочтительно: хранить только SHA-256-хэш ключа
        scopes: [write, read]      # write | read | admin
        collections: ["crm.*"]     # glob-маска; "*" — все коллекции
      - key: "hm_dev_plaintext"    # plaintext принимается для разработки, выдаёт warning
        scopes: [admin]
        collections: ["*"]
```

---

## Настройка тенантов и API-ключей

Каждый тенант получает свою базу MongoDB (`hm_t_{id}` на кластере по умолчанию или на указанном вами). Коллекции и индексы создаются автоматически при первой записи, если `collections.auto_create: true`.

### Области видимости ключей

| Область | Допустимые операции |
|---|---|
| `write` | Приём событий (state, diff, delete, batch) |
| `read` | Чтение истории, текущего состояния, point-in-time, коллекций, флоу |
| `admin` | Конфигурация коллекций, CRUD правил, управление DLQ + всё из `read` |

Маска `collections` ключа — это glob-паттерн. `"crm.*"` разрешает доступ к `crm.deals`, `crm.contacts` и т.д. `"*"` — все коллекции.

### Генерация хэша ключа

Храните хэши ключей вместо plaintext. Для хэширования:

```sh
echo -n "your-secret-key" | sha256sum | awk '{print "sha256:"$1}'
```

Используйте результат `sha256:<hex>` как значение `key_hash` в конфиге.

---

## Запуск в разных ролях

Один бинарь может обслуживать все роли или быть разделён для масштабирования:

```sh
# Всё в одном (по умолчанию)
./bin/letopis serve

# Только API-сервер (без воркера)
LETOPIS_ROLE=api ./bin/letopis serve

# Только воркер
LETOPIS_ROLE=worker ./bin/letopis serve
```

Для продакшна: несколько `api`-процессов за балансировщиком и один или несколько `worker`-процессов, читающих из очереди Redis Streams.

> **Важно:** Драйвер очереди `memory` работает только с `role: all`. Для любого разделения процессов используйте `redis-streams`.

---

## TLS

Оба сервера (HTTP и gRPC) поддерживают TLS. Укажите пути к сертификату и ключу:

```yaml
server:
  http:
    addr: ":443"
    tls:
      enabled: true
      cert_file: "/etc/letopis/tls/server.crt"
      key_file:  "/etc/letopis/tls/server.key"
  grpc:
    addr: ":9443"
    tls:
      enabled: true
      cert_file: "/etc/letopis/tls/server.crt"
      key_file:  "/etc/letopis/tls/server.key"
```

---

## Здоровье и наблюдаемость

| Ручка | Протокол | Описание |
|---|---|---|
| `/healthz` | HTTP | Liveness — всегда 200, пока процесс жив |
| `/readyz` | HTTP | Readiness — проверяет подключение к MongoDB и Redis |
| `/metrics` | HTTP | Метрики Prometheus |
| `/version` | HTTP | JSON с информацией о сборке |
| `:9090` | gRPC | `letopis.v1.SystemService` + стандартный health + reflection |

Метрики Prometheus включают глубину очереди, отставание консьюмера, скорости ingest по тенантам и режимам, backpressure-счётчики и счётчики ошибок доставки. Примеры алертов — в [`deploy/prometheus/letopis-alerts.yml`](../../deploy/prometheus/letopis-alerts.yml).

---

## Клиентские SDK

Если ваше приложение на PHP/Laravel или Node.js/TypeScript, не собирайте HTTP-запросы
вручную — используйте официальный SDK:

```bash
composer require letopis/laravel-sdk   # Laravel 11/12
npm install letopis-node               # Node.js 18+
```

Подробнее: [Клиентские SDK](sdks.md).

---

## Дальнейшие шаги

- [Концепции](concepts.md) — коллекции, режимы надёжности, мультитенантность
- [API записи](write-api.md) — приём событий
- [API чтения](read-api.md) — запросы истории и point-in-time реконструкция
- [Admin API](admin-api.md) — конфигурация коллекций, правила и вебхуки
- [Клиентские SDK](sdks.md) — официальные Laravel и Node.js SDK
