package handlers

import (
	"context"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/tafolabi009/backend/go_backend/pkg/grpcclient"
	"github.com/tafolabi009/backend/go_backend/pkg/logger"
)

// HealthHandler provides health check endpoints
type HealthHandler struct {
	db               *pgxpool.Pool
	redis            *redis.Client
	validationClient *grpcclient.ValidationClient
	log              *logger.Logger
}

// NewHealthHandler creates a new health handler
func NewHealthHandler(db *pgxpool.Pool, redis *redis.Client, validationClient *grpcclient.ValidationClient) *HealthHandler {
	return &HealthHandler{
		db:               db,
		redis:            redis,
		validationClient: validationClient,
		log:              logger.Get().With("handler", "health"),
	}
}

// HealthResponse represents health check response
type HealthResponse struct {
	Status    string                   `json:"status"`
	Timestamp string                   `json:"timestamp"`
	Services  map[string]ServiceHealth `json:"services"`
	Version   string                   `json:"version"`
}

// ServiceHealth represents individual service health
type ServiceHealth struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
	Latency string `json:"latency,omitempty"`
}

// Health performs basic liveness check
func (h *HealthHandler) Health(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"status":    "healthy",
		"timestamp": time.Now().Format(time.RFC3339),
	})
}

// ReadinessCheck performs comprehensive readiness check
func (h *HealthHandler) ReadinessCheck(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 10*time.Second)
	defer cancel()

	traceID := c.Locals("trace_id")
	log := h.log.With("trace_id", traceID)

	services := make(map[string]ServiceHealth)
	overallHealthy := true

	// Check PostgreSQL
	dbHealth := h.checkDatabase(ctx)
	services["database"] = dbHealth
	if dbHealth.Status != "healthy" {
		overallHealthy = false
	}

	// Check Redis
	redisHealth := h.checkRedis(ctx)
	services["redis"] = redisHealth
	if redisHealth.Status != "healthy" {
		overallHealthy = false
	}

	// Check Validation Service
	validationHealth := h.checkValidationService(ctx)
	services["validation_service"] = validationHealth
	if validationHealth.Status != "degraded" && validationHealth.Status != "healthy" {
		overallHealthy = false
	}

	status := "healthy"
	if !overallHealthy {
		status = "unhealthy"
		log.Warn("Readiness check failed", "services", services)
	}

	response := HealthResponse{
		Status:    status,
		Timestamp: time.Now().Format(time.RFC3339),
		Services:  services,
		Version:   "1.0.0",
	}

	statusCode := fiber.StatusOK
	if !overallHealthy {
		statusCode = fiber.StatusServiceUnavailable
	}

	return c.Status(statusCode).JSON(response)
}

// checkDatabase checks PostgreSQL connectivity
func (h *HealthHandler) checkDatabase(ctx context.Context) ServiceHealth {
	start := time.Now()

	err := h.db.Ping(ctx)
	latency := time.Since(start)

	if err != nil {
		h.log.Error("Database health check failed", "error", err)
		return ServiceHealth{
			Status:  "unhealthy",
			Message: "Failed to connect to database",
			Latency: latency.String(),
		}
	}

	// Check pool stats
	stats := h.db.Stat()
	if stats.TotalConns() == 0 {
		return ServiceHealth{
			Status:  "unhealthy",
			Message: "No database connections available",
			Latency: latency.String(),
		}
	}

	return ServiceHealth{
		Status:  "healthy",
		Latency: latency.String(),
	}
}

// checkRedis checks Redis connectivity
func (h *HealthHandler) checkRedis(ctx context.Context) ServiceHealth {
	if h.redis == nil {
		return ServiceHealth{
			Status:  "disabled",
			Message: "Redis not configured",
		}
	}

	start := time.Now()

	err := h.redis.Ping(ctx).Err()
	latency := time.Since(start)

	if err != nil {
		h.log.Error("Redis health check failed", "error", err)
		return ServiceHealth{
			Status:  "unhealthy",
			Message: "Failed to connect to Redis",
			Latency: latency.String(),
		}
	}

	return ServiceHealth{
		Status:  "healthy",
		Latency: latency.String(),
	}
}

// checkValidationService checks ML service health
func (h *HealthHandler) checkValidationService(ctx context.Context) ServiceHealth {
	if h.validationClient == nil {
		return ServiceHealth{
			Status:  "disabled",
			Message: "Validation service not configured",
		}
	}

	start := time.Now()

	err := h.validationClient.Health(ctx, "health-check")
	latency := time.Since(start)

	if err != nil {
		// Circuit breaker might be open - this is degraded, not unhealthy
		if latency < 100*time.Millisecond {
			return ServiceHealth{
				Status:  "degraded",
				Message: "Circuit breaker open, requests will be rejected",
				Latency: latency.String(),
			}
		}

		h.log.Error("Validation service health check failed", "error", err)
		return ServiceHealth{
			Status:  "unhealthy",
			Message: "Failed to connect to validation service",
			Latency: latency.String(),
		}
	}

	return ServiceHealth{
		Status:  "healthy",
		Latency: latency.String(),
	}
}

// RegisterRoutes registers health check routes
func (h *HealthHandler) RegisterRoutes(app *fiber.App) {
	app.Get("/health", h.Health)
	app.Get("/health/ready", h.ReadinessCheck)
}
