# Developer Quick Start - Updated System

## What Changed?

**DELETED:** All Gin code (1,500+ lines of duplicates)  
**ADDED:** Production infrastructure (logging, errors, circuit breakers, persistence, health checks)  
**UPGRADED:** 5.5/10 → 8.5/10

## New Infrastructure You Must Use

### 1. Structured Logging (REQUIRED)
```go
import "github.com/yourusername/go_backend/pkg/logger"

// Initialize once in main.go
logger.Init("info", "production")

// In handlers/services
log := logger.Get().With("trace_id", c.Locals("trace_id"), "user_id", userID)
log.Info("Processing request", "dataset", path, "size", bytes)
log.Error("Failed to process", "error", err, "attempts", retries)

// Before returning
defer logger.Get().Sync()
```

**Never use:** `log.Printf()`, `fmt.Println()`, or `logger` from stdlib

### 2. Standardized Errors (REQUIRED)
```go
import "github.com/yourusername/go_backend/pkg/errors"

// In handlers
func (h *Handler) CreateJob(c *fiber.Ctx) error {
    traceID := c.Locals("trace_id")
    
    if err := validateInput(req); err != nil {
        return errors.BadRequest("Invalid input", err, traceID)
    }
    
    job, err := h.service.CreateJob(ctx, req)
    if err != nil {
        return errors.InternalServerError("Failed to create job", err, traceID)
    }
    
    return c.JSON(job)
}

// Error codes available:
// BadRequest(), Unauthorized(), Forbidden(), NotFound()
// Conflict(), ValidationFailed(), TooManyRequests()
// ServiceUnavailable(), GatewayTimeout(), InsufficientStorage()
// DatabaseError(), InternalServerError()
```

### 3. Circuit Breakers (REQUIRED for external services)
```go
import "github.com/yourusername/go_backend/pkg/circuitbreaker"

// Create once per service
validationCB := circuitbreaker.NewCircuitBreaker(
    "validation-service",
    circuitbreaker.DefaultConfig(),
)

// Wrap all external calls
result, err := validationCB.Execute(ctx, func() (interface{}, error) {
    return mlClient.TrainCascade(ctx, request)
})

if err != nil {
    log.Error("Circuit breaker prevented call", "error", err)
    // Implement fallback logic here
    return nil, errors.ServiceUnavailable("ML service unavailable", err, traceID)
}

response := result.(*pb.TrainCascadeResponse)
```

### 4. Job Persistence (REQUIRED)
```go
import "github.com/yourusername/go_backend/internal/repository"

// Initialize in main.go
jobRepo := repository.NewJobRepository(dbPool)

// Create job
job := &models.Job{
    ID:           uuid.New().String(),
    UserID:       userID,
    Type:         "validation",
    Status:       "pending",
    DatasetPath:  path,
    Config:       configMap,
    CreatedAt:    time.Now(),
    UpdatedAt:    time.Now(),
}
err := jobRepo.CreateJob(ctx, job)

// Update progress
err = jobRepo.UpdateJobStatus(ctx, jobID, "running", 50.0, "")

// Store results
resultMap := map[string]interface{}{
    "accuracy": 0.95,
    "models": []string{"tier1", "tier2"},
}
err = jobRepo.UpdateJobResult(ctx, jobID, resultMap)

// List user jobs
jobs, err := jobRepo.ListJobs(ctx, userID, 10, 0)
```

### 5. Trace IDs (AUTOMATIC)
```go
// Middleware adds trace ID automatically
app.Use(middleware.TraceID())

// Access in handlers
traceID := c.Locals("trace_id")

// Pass to services
ctx = context.WithValue(c.Context(), "trace_id", traceID)
result, err := service.Process(ctx, data)

// Propagate to ML services (automatic in gRPC client)
```

### 6. Rate Limiting (REQUIRED)
```go
import "github.com/yourusername/go_backend/internal/middleware"

// In main.go - global rate limit
app.Use(middleware.RateLimit()) // 100 req/min per IP

// Per-endpoint custom limits
app.Post("/api/v1/jobs/validate",
    middleware.RateLimitWithConfig(10, 60*time.Second), // 10 req/min
    handler.CreateValidationJob,
)
```

### 7. Health Checks (REQUIRED)
```go
import "github.com/yourusername/go_backend/internal/handlers"

// In main.go
healthHandler := handlers.NewHealthHandler(dbPool, redisClient, validationClient)
healthHandler.RegisterRoutes(app)

// Kubernetes liveness
livenessProbe:
  httpGet:
    path: /health
    port: 8080

// Kubernetes readiness
readinessProbe:
  httpGet:
    path: /health/ready
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 5
```

### 8. Connection Pooling (AUTOMATIC)
```go
import "github.com/yourusername/go_backend/pkg/database"

// In main.go
poolConfig := database.DefaultPoolConfig()
poolConfig.MaxConns = 25  // Adjust based on load

dbPool, err := database.NewPool(ctx, databaseURL, poolConfig)
if err != nil {
    log.Fatal("Failed to create pool", "error", err)
}
defer dbPool.Close()

// Monitor pool (optional)
go database.MonitorPool(ctx, dbPool, 30*time.Second)
```

### 9. ML Service Integration
```go
import "github.com/yourusername/go_backend/pkg/grpcclient"

// In main.go
validationClient, err := grpcclient.NewValidationClient("localhost:50051")
if err != nil {
    log.Fatal("Failed to connect to validation service", "error", err)
}
defer validationClient.Close()

// In orchestrator service
func (s *OrchestratorService) TrainCascade(ctx context.Context, jobID, datasetPath string) error {
    // Circuit breaker is automatic in client
    response, err := s.validationClient.TrainCascade(ctx, jobID, datasetPath, &pb.TrainingConfig{
        NumEpochs:     50,
        BatchSize:     32,
        LearningRate:  0.001,
        UseMultiGpu:   true,
        NumGpus:       2,
    })
    
    if err != nil {
        return fmt.Errorf("training failed: %w", err)
    }
    
    // Store results
    resultMap := map[string]interface{}{
        "status":        response.Status,
        "avg_accuracy":  response.Metrics.AverageAccuracy,
        "best_accuracy": response.Metrics.BestAccuracy,
        "best_model":    response.Metrics.BestModel,
    }
    
    return s.jobRepo.UpdateJobResult(ctx, jobID, resultMap)
}
```

## Database Migrations

### Run Migrations
```bash
# Install migrate CLI
go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest

# Run migrations
cd /workspaces/ml_backend/go_backend
migrate -path migrations -database "postgresql://user:pass@localhost:5432/synthos?sslmode=disable" up

# Rollback
migrate -path migrations -database "${DATABASE_URL}" down 1

# Check version
migrate -path migrations -database "${DATABASE_URL}" version
```

### Create New Migration
```bash
migrate create -ext sql -dir migrations -seq add_new_table
# Edit: migrations/000002_add_new_table.up.sql
# Edit: migrations/000002_add_new_table.down.sql
```

## Environment Variables

### Required
```env
DATABASE_URL=postgresql://user:pass@localhost:5432/synthos?sslmode=disable
VALIDATION_SERVICE_ADDR=localhost:50051
JWT_SECRET=your-secret-here  # CHANGE IN PRODUCTION
```

### Optional
```env
# Logging
LOG_LEVEL=info
ENV=production

# Redis (for distributed rate limiting)
REDIS_URL=redis://localhost:6379

# Rate Limiting
RATE_LIMIT_REQUESTS=100
RATE_LIMIT_WINDOW=60

# Connection Pool
DB_MAX_CONNS=25
DB_MIN_CONNS=5

# ML Service
MAX_CONCURRENT_JOBS=5
GRPC_MAX_WORKERS=10
```

## Testing Your Changes

### Unit Tests
```bash
# Test specific package
go test ./pkg/logger -v
go test ./pkg/errors -v
go test ./internal/repository -v

# Test with coverage
go test ./... -cover

# Race detection
go test ./... -race
```

### Integration Tests
```bash
# Start dependencies
docker-compose up -d postgres redis

# Run migrations
migrate -path migrations -database "${DATABASE_URL}" up

# Run tests
go test ./tests/integration_test.go -v
```

### Manual Testing
```bash
# Health check
curl http://localhost:8080/health/ready | jq

# Create job
curl -X POST http://localhost:8080/api/v1/jobs/validate \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${TOKEN}" \
  -d '{
    "dataset_path": "/data/test.parquet",
    "config": {
      "num_epochs": 50,
      "batch_size": 32
    }
  }' | jq

# Check job status
curl http://localhost:8080/api/v1/jobs/${JOB_ID} \
  -H "Authorization: Bearer ${TOKEN}" | jq

# Check trace ID propagation (look for X-Trace-ID in response headers)
curl -v http://localhost:8080/health
```

## Common Patterns

### Handler Template
```go
func (h *Handler) CreateResource(c *fiber.Ctx) error {
    // 1. Get trace ID
    traceID := c.Locals("trace_id")
    log := logger.Get().With("trace_id", traceID, "handler", "create_resource")
    
    // 2. Parse request
    var req ResourceRequest
    if err := c.BodyParser(&req); err != nil {
        return errors.BadRequest("Invalid JSON", err, traceID)
    }
    
    // 3. Validate (TODO: add go-playground/validator)
    if req.Name == "" {
        return errors.ValidationFailed("Name is required", nil, traceID)
    }
    
    // 4. Get user from context
    userID := c.Locals("user_id").(string)
    
    // 5. Call service with context
    ctx := context.WithValue(c.Context(), "trace_id", traceID)
    resource, err := h.service.Create(ctx, userID, &req)
    if err != nil {
        log.Error("Failed to create resource", "error", err)
        return errors.InternalServerError("Creation failed", err, traceID)
    }
    
    // 6. Log success
    log.Info("Resource created", "resource_id", resource.ID)
    
    // 7. Return response
    return c.Status(fiber.StatusCreated).JSON(resource)
}
```

### Service Template
```go
func (s *Service) Process(ctx context.Context, data *Data) (*Result, error) {
    traceID := ctx.Value("trace_id")
    log := logger.Get().With("trace_id", traceID, "service", "process")
    
    // 1. Persist job
    job := &models.Job{...}
    if err := s.jobRepo.CreateJob(ctx, job); err != nil {
        return nil, fmt.Errorf("failed to create job: %w", err)
    }
    
    // 2. Call external service with circuit breaker (automatic in gRPC client)
    result, err := s.mlClient.TrainCascade(ctx, job.ID, data.Path, config)
    if err != nil {
        // Update job status
        s.jobRepo.UpdateJobStatus(ctx, job.ID, "failed", 0, err.Error())
        return nil, fmt.Errorf("training failed: %w", err)
    }
    
    // 3. Update job with results
    resultMap := convertToMap(result)
    if err := s.jobRepo.UpdateJobResult(ctx, job.ID, resultMap); err != nil {
        log.Error("Failed to update job result", "error", err)
    }
    
    log.Info("Processing completed", "job_id", job.ID)
    return result, nil
}
```

## Troubleshooting

### Circuit Breaker Open
```
Error: "ML service unavailable"
Status: 503
```
**Fix:** Check ML service logs, verify connectivity, wait for circuit to close (30s)

### Rate Limit Exceeded
```
Error: "Too many requests"
Status: 429
```
**Fix:** Reduce request rate or increase limit in middleware config

### Database Pool Exhausted
```
Error: "Failed to acquire connection"
Status: 500
```
**Fix:** Increase `DB_MAX_CONNS` or optimize slow queries

### Missing Trace ID
```
Error: trace_id not in logs
```
**Fix:** Ensure `TraceID()` middleware is registered before handlers

## What NOT to Do

❌ Don't use `log.Printf()` - use structured logger  
❌ Don't return raw errors - use standardized errors  
❌ Don't call external services without circuit breaker  
❌ Don't store jobs in memory - use job repository  
❌ Don't forget to add rate limiting to new endpoints  
❌ Don't ignore health check failures  
❌ Don't hardcode secrets - use environment variables  

## Getting Help

1. Check `/CRITICAL_FIXES_COMPLETE.md` for full documentation
2. Look at existing handlers for patterns
3. Check logs with trace ID: `grep "trace_id=abc-123" /var/log/app.log`
4. Verify health: `curl http://localhost:8080/health/ready`
5. Check circuit breaker state in logs

## Next Steps for New Features

1. Add handler using template above
2. Add rate limiting middleware
3. Implement service with circuit breakers
4. Add job persistence if long-running
5. Add structured logging
6. Write unit tests
7. Write integration tests
8. Update health checks if new dependency
9. Document API endpoint
10. Deploy!
