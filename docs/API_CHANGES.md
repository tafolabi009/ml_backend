# API Changes — Rename, Paddle Billing, Admin Metrics & Impersonation

Authoritative contract for the frontend session. All routes under `/api/v1`
except the Paddle webhook. Bearer JWT auth unless marked public.

## 1. Editable validation names

**`PATCH /api/v1/validations/{id}`** — body `{"name": string}` — owner-only.

- Rules: trimmed, 1–120 chars, non-blank, no uniqueness. `400 INVALID_NAME`
  on violation.
- Returns the full updated validation object (same shape as `GET
  /validations/{id}`), now including `"name"` when set.
- The name propagates to: the validation detail response (`name`), the report
  PDF header, the signed certificate payload (`validation_name` field), and
  datasheets.
- Audited in `security_events` as `validation_renamed`.

## 2. Paddle billing (server side)

**`POST /webhooks/paddle`** — public, HMAC-SHA256 signature-verified.

- Header `Paddle-Signature: ts=<unix>;h1=<hex>`; digest is
  `HMAC(secret, "<ts>:<rawbody>")`. Timestamps older/newer than 5 min are
  rejected. Secret from `PADDLE_WEBHOOK_SECRET`. Payloads are never logged.
- `transaction.completed`: provisions credits **idempotently** (dedupe on the
  Paddle transaction id via `reference_type='paddle'` + the `uq_paddle_txn`
  partial unique index) and records `receipt_url` into the transaction
  metadata. Package resolved from `custom_data.package_id`, else from
  `items[].price.id` → `credit_packages.paddle_price_id`.
- `transaction.refunded` / `adjustment.created`: idempotent claw-back of the
  original grant.
- Unhandled event types return `200 {"received": true}` so Paddle stops
  retrying.

**Transactions API** (`GET /api/v1/credits/history`): each transaction now
carries `"receipt_url"` (nullable; populated once the webhook records it).

**Checkout wiring**: `credit_packages.paddle_price_id` (new column) maps a
package to its Paddle price; the frontend opens Paddle checkout with that
price id and passes `custom_data: {user_id, package_id}` so the webhook can
provision. No server-side checkout-session endpoint is required for Paddle's
hosted overlay — if a hosted-link flow is later needed, it will be added as
`POST /api/v1/credits/checkout` returning `{checkout_url}`.

## 3. Admin metrics + impersonation (admin-only)

**`GET /api/v1/admin/metrics`** — query `metric=signups|validations|revenue`,
`from=<ISO8601|YYYY-MM-DD>`, `to=<...>`, `granularity=day|week|month`
(default day / last 30 days). Response:

```json
{"metric": "signups", "granularity": "day",
 "series": [{"bucket": "2026-07-01T00:00:00Z", "value": 12}]}
```

`revenue` is in cents, summed from Paddle-provisioned purchases joined to
package prices.

**`POST /api/v1/admin/impersonate`** — body `{"user_id"}`. Response:

```json
{"token": "<jwt>", "expires_at": "<ISO8601>", "impersonator_id": "<admin id>"}
```

- Token TTL is 15 minutes and carries an `impersonator_id` claim.
- Cannot impersonate yourself or another admin (`400`/`403`).
- **Every request made with the token is audit-logged** in `security_events`
  (`impersonated_request`, with both identities) and the session is
  **read-mostly**: any non-GET/HEAD/OPTIONS request (except `POST
  /auth/logout`) returns `403 IMPERSONATION_READ_ONLY`. The frontend should
  render a persistent banner from the `impersonator_id` claim.

## 4. Python SDK + CLI

`sdk/python` (`synthos` package) gained: `rename_validation`,
`list_validations`, `get_findings`, `create_group_validation`, directory
uploads via dataset groups, and a `group_name` option on `upload_dataset`.
CLI: `synthos validate <path-or-dir>` (a directory validates as one group),
plus `synthos rename <id> <name>`.

## Deviations from the requested contract

- **No standalone Paddle checkout-session endpoint.** Paddle's hosted overlay
  is opened client-side with the `paddle_price_id`; the server's role is the
  webhook + price mapping. Documented above; add `POST /credits/checkout`
  later if a server-minted hosted link is needed.
- **`revenue` metric derives from package prices**, not a stored per-txn
  amount (there is no amount column on `credit_transactions`). Accurate for
  package purchases; promo/refund adjustments are excluded by design.
- **Impersonation blocks all writes**, including non-billing ones, rather than
  maintaining a curated allow-list — simplest safe default. Loosen per route
  if a real read-write impersonation need appears.
- Migrations: `migrations/000004_rename_and_paddle.{up,down}.sql` (also
  applied idempotently by the startup migration runner).
