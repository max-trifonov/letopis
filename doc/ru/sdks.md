# Клиентские SDK

Официальные SDK оборачивают REST API типизированным клиентом, билдерами запросов и
интеграцией с фреймворком — не нужно вручную собирать HTTP-запросы и JSON.

| SDK | Стек | Пакет | Репозиторий |
|---|---|---|---|
| Laravel SDK | PHP 8.2+, Laravel 11/12 | [`letopis/laravel-sdk`](https://packagist.org/packages/letopis/laravel-sdk) (Packagist) | [max-trifonov/letopis-laravel-sdk](https://github.com/max-trifonov/letopis-laravel-sdk) |
| Node SDK | Node.js 18+, TypeScript 5+ | [`letopis-node`](https://www.npmjs.com/package/letopis-node) (npm) | [max-trifonov/letopis-node-sdk](https://github.com/max-trifonov/letopis-node-sdk) |

Нет SDK под ваш стек? Оба SDK построены поверх REST API — см. [API записи](write-api.md)
и [API чтения](read-api.md), либо сгенерируйте клиент из [OpenAPI 3.1-спецификации](../../api/openapi/letopis.v1.yaml).

---

## Laravel SDK

```bash
composer require letopis/laravel-sdk
php artisan vendor:publish --tag=letopis-config
```

```env
LETOPIS_BASE_URL=https://your-letopis-instance.example.com
LETOPIS_API_KEY=hm_live_...
```

```php
use Letopis\Laravel\Facades\Letopis;

Letopis::ingest('crm.deals', 'd-1')
    ->authorId('42')
    ->state(['title' => 'Deal #1', 'amount' => 250, 'status' => 'open']);

$history = Letopis::history('crm.deals', 'd-1')->limit(50)->get();
```

Включает Eloquent-обсервер (трейт `HasLetopisHistory` или конфиг без изменения моделей)
для автоматической записи изменений, а также хелпер `Letopis::fake()` для тестов.
Покрывает ingest, батч, историю, point-in-time чтение, активности/флоу, правила,
верификацию подписи вебхука, hash-chain `:verify` и admin-эндпоинты.

Полная документация: [github.com/max-trifonov/letopis-laravel-sdk](https://github.com/max-trifonov/letopis-laravel-sdk)

---

## Node SDK

```bash
npm install letopis-node
```

```typescript
import { LetopisClient } from 'letopis-node'

const letopis = new LetopisClient({
  baseUrl: 'https://your-letopis-instance.example.com',
  apiKey: 'hm_live_...',
})

await letopis.ingest('crm.deals', 'd-1')
  .authorId('42')
  .state({ title: 'Deal #1', amount: 250, status: 'open' })

const history = await letopis.history('crm.deals', 'd-1').limit(50).get()
```

Типизированные билдеры запросов для ingest, батча, истории, point-in-time чтения,
активностей/флоу, правил и admin-эндпоинтов. Без рантайм-зависимостей (нативный `fetch`).

Полная документация: [github.com/max-trifonov/letopis-node-sdk](https://github.com/max-trifonov/letopis-node-sdk)
