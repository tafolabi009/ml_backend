# Team Workspaces — Phase 2 Design

Goal: orgs share datasets, validations, monitors and a credit pool, with
role-gated access (owner / member / viewer). The frontend reuses the
invite-flow patterns already built for the admin section.

## Schema (additive; created by the startup migration runner)

```sql
CREATE TABLE IF NOT EXISTS organizations (
    id           VARCHAR(255) PRIMARY KEY,          -- org_<uuid8>
    name         VARCHAR(255) NOT NULL,
    owner_id     VARCHAR(255) NOT NULL REFERENCES users(id),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS organization_members (
    org_id       VARCHAR(255) NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    user_id      VARCHAR(255) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role         VARCHAR(20)  NOT NULL DEFAULT 'member',  -- owner|member|viewer
    invited_by   VARCHAR(255),
    joined_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (org_id, user_id)
);

CREATE TABLE IF NOT EXISTS organization_invites (
    id           VARCHAR(255) PRIMARY KEY,          -- oinv_<uuid8>
    org_id       VARCHAR(255) NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    email        VARCHAR(255) NOT NULL,
    role         VARCHAR(20)  NOT NULL DEFAULT 'member',
    token_hash   VARCHAR(255) NOT NULL,             -- emailed token, hashed at rest
    invited_by   VARCHAR(255) NOT NULL,
    expires_at   TIMESTAMPTZ NOT NULL,
    accepted_at  TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Resource sharing: nullable org_id on existing tables (NULL = personal).
ALTER TABLE datasets        ADD COLUMN IF NOT EXISTS org_id VARCHAR(255);
ALTER TABLE validations     ADD COLUMN IF NOT EXISTS org_id VARCHAR(255);
ALTER TABLE dataset_monitors ADD COLUMN IF NOT EXISTS org_id VARCHAR(255);
CREATE INDEX IF NOT EXISTS idx_datasets_org    ON datasets(org_id);
CREATE INDEX IF NOT EXISTS idx_validations_org ON validations(org_id);

-- Shared credit pool: one balance row per org, same ledger mechanics.
ALTER TABLE credit_balances     ADD COLUMN IF NOT EXISTS org_id VARCHAR(255) UNIQUE;
ALTER TABLE credit_transactions ADD COLUMN IF NOT EXISTS org_id VARCHAR(255);
```

## Access model

- Every request resolves an **actor context**: `user_id` + optional
  `X-Org-ID` header (validated against organization_members).
- Ownership checks generalize from `resource.user_id == userID` to
  `resource.user_id == userID OR (resource.org_id == orgID AND member)`.
  One helper (`canAccess(resource, actor, write bool)`) replaces the
  scattered checks; `viewer` fails all write paths.
- Charging: if the request runs in org context, `CreateValidationCharged`
  targets the org's balance row (`org_id`) instead of the user's; the
  transaction records both `user_id` (who acted) and `org_id` (who paid).

## Endpoints

```
POST   /orgs                      create (creator becomes owner)
GET    /orgs                      list my orgs
GET    /orgs/:id                  detail + members
PATCH  /orgs/:id                  rename (owner)
POST   /orgs/:id/invites          {email, role} → emails a token (owner)
GET    /orgs/:id/invites          pending invites (owner)
DELETE /orgs/:id/invites/:inv     revoke (owner)
POST   /orgs/invites/accept      {token} → join
PATCH  /orgs/:id/members/:uid     change role (owner)
DELETE /orgs/:id/members/:uid     remove (owner) / leave (self)
GET    /orgs/:id/credits          shared balance + ledger
```

## Rollout order

1. Schema + org CRUD + membership + invite flow (email via existing Resend).
2. Actor-context middleware + `canAccess` helper; migrate dataset/validation
   list & detail handlers to it (personal behavior unchanged when no org).
3. Shared credit pool wiring in the charged-create paths + billing UI.
4. Monitors/schedules/warranties inherit org context (they key off datasets).
5. Frontend: org switcher in the sidebar; invite management page cloned from
   the admin invites UI.

## Non-goals for the first cut

- SSO/SCIM group sync (separate enterprise track).
- Per-resource ACLs beyond the three roles.
- Cross-org transfer of resources.
