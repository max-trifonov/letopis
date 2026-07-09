# Client SDKs

Official SDKs wrap the REST API with a typed client, request builders, and framework
integration, so you don't hand-roll HTTP calls and JSON shapes.

| SDK | Stack | Package | Repository |
|---|---|---|---|
| Laravel SDK | PHP 8.2+, Laravel 11/12 | [`letopis/laravel-sdk`](https://packagist.org/packages/letopis/laravel-sdk) (Packagist) | [max-trifonov/letopis-laravel-sdk](https://github.com/max-trifonov/letopis-laravel-sdk) |
| Node SDK | Node.js 18+, TypeScript 5+ | [`letopis-node`](https://www.npmjs.com/package/letopis-node) (npm) | [max-trifonov/letopis-node-sdk](https://github.com/max-trifonov/letopis-node-sdk) |

No SDK for your stack yet? The REST API is the contract both SDKs are built on — see
[Write API](write-api.md) and [Read API](read-api.md), or generate a client from the
[OpenAPI 3.1 spec](../api/openapi/letopis.v1.yaml).

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

Includes an Eloquent model observer (`HasLetopisHistory` trait or config-driven, no model
changes needed) to record changes automatically, plus a `Letopis::fake()` testing helper.
Covers ingest, batch, history, point-in-time reads, activities/flows, rules, webhook
signature verification, hash-chain `:verify`, and admin endpoints.

Full docs: [github.com/max-trifonov/letopis-laravel-sdk](https://github.com/max-trifonov/letopis-laravel-sdk)

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

Typed request builders for ingest, batch, history, point-in-time reads, activities/flows,
rules, and admin endpoints. Zero runtime dependencies (native `fetch`).

Full docs: [github.com/max-trifonov/letopis-node-sdk](https://github.com/max-trifonov/letopis-node-sdk)
