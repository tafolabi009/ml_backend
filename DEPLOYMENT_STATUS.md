# SynthOS AWS Deployment Status

**Region:** `us-east-1` · **Account:** `173128528397` · **Date:** 2026-06-23

## Live API endpoint

```
https://api.synthos.dev      (HTTPS via ACM; HTTP -> HTTPS 301 redirect)
http://synthos-alb-1146484742.us-east-1.elb.amazonaws.com   (direct ALB DNS)
```

| Path | Result |
|------|--------|
| `GET /health/live` | `{"status":"alive"}` (200) |
| `GET /health` | `{"status":"healthy","database":"healthy", ...}` (200) |
| `GET /health/ready` | `{"ready":true}` (200) |
| `GET /api/v1/datasets` | `401` (auth required — routing + auth middleware live) |

## What is deployed

### Phase 1 — Foundation (pre-existing, healthy)
- VPC `vpc-07051ce77c47176a6` (10.0.0.0/16); public subnets `subnet-01474200959398065`, `subnet-0eede9adbd5a6011b`; private `subnet-06030e7c0001e0d01`, `subnet-020be27aca53ab33b`
- Security groups: `synthos-alb-sg`, `synthos-app-sg`, `synthos-internal-sg`, `synthos-db-sg`, `synthos-redis-sg`
- **RDS PostgreSQL** `synthos-db` (`synthos-db.c8j8wqysmu4j.us-east-1.rds.amazonaws.com:5432`, db `synthosdb`, user `synthos_admin`) — available
- **ElastiCache Redis** `synthos-redis-001.synthos-redis.liiq3k.use1.cache.amazonaws.com:6379`
- S3: `synthos-datasets-us-east-1`, `synthos-models-us-east-1`, `synthos-reports-us-east-1`
- Secrets Manager: `synthos/production/secrets`

### Phase 3 — CPU/Fargate tier (deployed this session)
- ECR images built & pushed: `synthos/go-backend:latest`, `synthos/job-orchestrator:latest`
- ECS cluster `synthos-cluster` (Fargate):
  - **`synthos-go-backend`** — desired 2, **running 2**, behind ALB (task def `:2`)
  - **`synthos-job-orchestrator`** — desired 1, **running 1** (10 workers)
- **ALB** `synthos-alb` (`synthos-alb-1146484742...`), target group `synthos-go-backend-tg`, HTTP:80 listener
- IAM roles `synthos-ecs-task-execution`, `synthos-ecs-task-role`
- CloudWatch log groups `/synthos/go-backend`, `/synthos/job-orchestrator`

## What is NOT yet deployed

### ML backend — running on Fargate CPU (GPU quota DENIED; appeal pending)
- The g5.xlarge G/VT quota increase was **DENIED** by AWS (account has 0 GPU quota). An **appeal** with a detailed use case is to be submitted (reopen the case).
- **Stopgap (live):** `synthos-ml-backend` runs on **Fargate CPU** (4 vCPU / 16 GB, `GPU_TIER=cpu`, `CUDA_VISIBLE_DEVICES=-1`), registered in Cloud Map as `ml-backend.synthos`. Serves ValidationEngine (`:50051`), CollapseEngine (`:50052`). Slower than GPU but functional.
- **Real TEN engine installed:** the ML Dockerfile now installs `ten` from `github.com/genovotechnologies/temporal-eigenstate-networks` (user-confirmed) with a build-time `import ten` assertion — runtime logs `Successfully imported ... (ten) package` (no more mock fallback). The legacy `NEURON_NEW` install was the bug.
- `go-backend` (`:6`) and `job-orchestrator` (`:2`) point `VALIDATION_/COLLAPSE_/DATA_SERVICE_ADDR` at `ml-backend.synthos`.
- **When GPU is granted (post-appeal):** launch a `g5.xlarge` (or run the ML container on a GPU EC2 node) and repoint the ML service addrs to it for full-speed inference.

## Corrections applied at deploy time (fold these into `scripts/aws/*`)

1. **Secret ARN suffix** — `phase3-ecs-fargate.sh` builds the secret `valueFrom` as
   `...:secret:synthos/production/secrets:database_url::` (no random suffix), which ECS rejects.
   Use the real ARN: `aws secretsmanager describe-secret --secret-id synthos/production/secrets --query ARN`
   (e.g. `...secret:synthos/production/secrets-yWFvGS:database_url::`).
2. **`CLOUD_PROVIDER=aws`** — must be set in the go-backend task env, otherwise it defaults toward GCS init and `log.Fatalf`.
3. **GPU instance type** — `phase2-gpu-node.sh` defaults to `p3.2xlarge` (retired in us-east-1). Use `g5.xlarge` (A10G) or `g4dn.xlarge` (T4).
4. **`phase2` `ML_BACKEND_IMAGE`** is referenced in the EC2 user-data but never assigned — set it to `${ECR_REPO}/${PROJECT_NAME}/ml-backend:latest`.
5. **Secrets Manager JSON** — the original `synthos/production/secrets` value was malformed (raw newline in a string) and had placeholder DB/Redis hostnames; it was rewritten with valid JSON and the real endpoints. The RDS master password was rotated as part of this.

## Custom domain + HTTPS — api.synthos.dev (LIVE)

`api.synthos.dev` is served over HTTPS via the ALB. DNS for `synthos.dev` is managed at
name.com (not Route 53), with two CNAMEs:
1. `api.synthos.dev` -> `synthos-alb-1146484742.us-east-1.elb.amazonaws.com`
2. ACM DNS-validation CNAME (`_a688293cc2a1e86ee9f8d98d79135548.api` host)

- ACM cert `arn:aws:acm:us-east-1:173128528397:certificate/a09457c5-77f9-4803-9a8b-9603797d6d94` — **ISSUED**
- ALB **HTTPS:443** listener (TLS policy `ELBSecurityPolicy-TLS13-1-2-2021-06`) -> go-backend target group
- ALB **HTTP:80** -> **HTTPS:443** 301 redirect
- Verified: `https://api.synthos.dev/health` = 200, `http://...` = 301

## Completed hardening (this session)
- **CORS**: `ALLOWED_ORIGINS` = `https://synthos.dev,https://www.synthos.dev,https://app.synthos.dev,https://api.synthos.dev`.
- **Redis AUTH**: generated a new token, updated the secret's `redis_password`/`redis_url` (DB/JWT preserved), and rotated the `synthos-redis` replication group to the same token (`ROTATE`). Redis is now active. Optional: a follow-up `SET` rotation invalidates the old token.
- **Email (Resend)**: `RESEND_API_KEY` added to the secret (`resend_api_key`) and injected into go-backend; `EMAIL_FROM_ADDRESS=noreply@synthos.dev`, `EMAIL_FROM_NAME=Synthos`. Startup now logs `Email client configured (Resend)`; OTP / verification emails send.
- **Service status board / Orchestrator**: `GET /api/v1/developer/services` now does **real TCP health probes** (was a stub returning `"available"`, which the frontend rendered as Down). Wired the gateway to the orchestrator via **Cloud Map** service discovery: created SD service `job-orchestrator` in namespace `synthos` (`ns-xtarnjzuapccvndo`), recreated the `synthos-job-orchestrator` ECS service with the registry, and set `ORCHESTRATOR_ADDR=job-orchestrator.synthos:8080` on go-backend. Orchestrator now reports Healthy; Validation/Collapse correctly report Down until the GPU node is up. (Current task def: `synthos-go-backend:5`.)
- **S3 bucket CORS**: browser-based presigned uploads/downloads from `https://synthos.dev` were blocked (buckets had no CORS config). Applied a scoped CORS policy (origins = synthos.dev family only; methods GET/PUT/POST/HEAD/DELETE; expose ETag) to `synthos-datasets-us-east-1` (uploads) and `synthos-reports-us-east-1` (downloads). Verified the OPTIONS preflight returns `Access-Control-Allow-Origin: https://synthos.dev`.

## Remaining follow-ups
- **GPU ML node (Phase 2)** — deploy once the g5.xlarge quota case is approved.
- **Service discovery**: the `synthos` private DNS namespace was still provisioning at deploy time; re-attach the `job-orchestrator` service registry once present.
- Tear down the temporary `synthos-deployer` IAM admin user when finished.
