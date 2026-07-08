package handlers

import (
	"context"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
)

// GetDevOverviewFiber returns developer dashboard overview with service health and error counts
func GetDevOverviewFiber(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db := database.GetDB()

	// Database health
	dbHealthy := database.IsHealthy()

	// Recent error count (last 24h from security_events or audit logs)
	var recentErrors int64
	_ = db.QueryRow(ctx,
		`SELECT COUNT(*) FROM security_events WHERE success = false AND created_at > NOW() - INTERVAL '24 hours'`,
	).Scan(&recentErrors)

	// Total validations today
	var validationsToday int64
	_ = db.QueryRow(ctx,
		`SELECT COUNT(*) FROM validations WHERE created_at > NOW() - INTERVAL '24 hours'`,
	).Scan(&validationsToday)

	// Total users
	var totalUsers int64
	_ = db.QueryRow(ctx, `SELECT COUNT(*) FROM users`).Scan(&totalUsers)

	return c.JSON(fiber.Map{
		"database_healthy":  dbHealthy,
		"recent_errors_24h": recentErrors,
		"validations_today": validationsToday,
		"total_users":       totalUsers,
		"uptime":            time.Now().Format(time.RFC3339),
	})
}

// probeTCP reports "healthy" if a TCP connection to addr succeeds within the
// timeout, otherwise "unhealthy". Accepts "host:port" or "http(s)://host:port/path".
func probeTCP(addr string) string {
	if addr == "" {
		return "unhealthy"
	}
	addr = strings.TrimPrefix(strings.TrimPrefix(addr, "https://"), "http://")
	if i := strings.IndexByte(addr, '/'); i >= 0 {
		addr = addr[:i]
	}
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return "unhealthy"
	}
	_ = conn.Close()
	return "healthy"
}

func getEnvOrDefaultDev(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// GetServicesStatusFiber checks the real health of each backend service.
func GetServicesStatusFiber(c *fiber.Ctx) error {
	services := make(map[string]fiber.Map)

	// Database (real connectivity check)
	dbStatus := "unhealthy"
	if database.IsHealthy() {
		dbStatus = "healthy"
	}
	services["database"] = fiber.Map{"status": dbStatus}

	// Pending validations (best-effort, from DB)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db := database.GetDB()
	var pendingValidations int64
	_ = db.QueryRow(ctx, `SELECT COUNT(*) FROM validations WHERE status IN ('pending', 'running')`).Scan(&pendingValidations)

	// Backend services (real TCP reachability probes)
	services["orchestrator"] = fiber.Map{
		"status": probeTCP(getEnvOrDefaultDev("ORCHESTRATOR_ADDR", "localhost:8080")),
	}
	services["validation"] = fiber.Map{
		"status":              probeTCP(getEnvOrDefaultDev("VALIDATION_SERVICE_ADDR", "localhost:50051")),
		"pending_validations": pendingValidations,
	}
	services["collapse"] = fiber.Map{
		"status": probeTCP(getEnvOrDefaultDev("COLLAPSE_SERVICE_ADDR", "localhost:50052")),
	}

	return c.JSON(fiber.Map{
		"services":   services,
		"checked_at": time.Now().Format(time.RFC3339),
	})
}

// GetAPIDocsFiber returns a comprehensive OpenAPI 3.0 spec for all API endpoints
func GetAPIDocsFiber(c *fiber.Ctx) error {
	spec := `{
  "openapi": "3.0.3",
  "info": {
    "title": "Synthos API",
    "description": "Synthos AI Validation Platform API",
    "version": "1.0.0"
  },
  "servers": [
    {"url": "/api/v1", "description": "API v1"}
  ],
  "paths": {
    "/auth/register": {
      "post": {
        "tags": ["Auth"],
        "summary": "Register a new user",
        "requestBody": {"content": {"application/json": {"schema": {"type": "object", "properties": {"email": {"type": "string"}, "password": {"type": "string"}, "full_name": {"type": "string"}, "company_name": {"type": "string"}}}}}},
        "responses": {"201": {"description": "User registered"}}
      }
    },
    "/auth/login": {
      "post": {
        "tags": ["Auth"],
        "summary": "Login",
        "requestBody": {"content": {"application/json": {"schema": {"type": "object", "properties": {"email": {"type": "string"}, "password": {"type": "string"}, "totp_code": {"type": "string"}}}}}},
        "responses": {"200": {"description": "Login successful"}}
      }
    },
    "/auth/logout": {
      "post": {
        "tags": ["Auth"],
        "summary": "Logout (requires auth)",
        "security": [{"bearerAuth": []}],
        "responses": {"200": {"description": "Logged out"}}
      }
    },
    "/auth/refresh": {
      "post": {
        "tags": ["Auth"],
        "summary": "Refresh access token",
        "requestBody": {"content": {"application/json": {"schema": {"type": "object", "properties": {"refresh_token": {"type": "string"}}}}}},
        "responses": {"200": {"description": "Token refreshed"}}
      }
    },
    "/auth/forgot-password": {
      "post": {
        "tags": ["Auth"],
        "summary": "Request password reset",
        "requestBody": {"content": {"application/json": {"schema": {"type": "object", "properties": {"email": {"type": "string"}}}}}},
        "responses": {"200": {"description": "Reset email sent"}}
      }
    },
    "/auth/reset-password": {
      "post": {
        "tags": ["Auth"],
        "summary": "Complete password reset",
        "requestBody": {"content": {"application/json": {"schema": {"type": "object", "properties": {"token": {"type": "string"}, "new_password": {"type": "string"}}}}}},
        "responses": {"200": {"description": "Password reset"}}
      }
    },
    "/auth/me": {
      "get": {
        "tags": ["Auth"],
        "summary": "Get current user profile",
        "security": [{"bearerAuth": []}],
        "responses": {"200": {"description": "User profile"}}
      },
      "put": {
        "tags": ["Auth"],
        "summary": "Update current user profile",
        "security": [{"bearerAuth": []}],
        "requestBody": {"content": {"application/json": {"schema": {"type": "object", "properties": {"full_name": {"type": "string"}, "username": {"type": "string"}, "company_name": {"type": "string"}, "job_title": {"type": "string"}, "phone": {"type": "string"}}}}}},
        "responses": {"200": {"description": "Profile updated"}}
      }
    },
    "/auth/change-password": {
      "post": {
        "tags": ["Auth"],
        "summary": "Change password",
        "security": [{"bearerAuth": []}],
        "requestBody": {"content": {"application/json": {"schema": {"type": "object", "properties": {"current_password": {"type": "string"}, "new_password": {"type": "string"}}}}}},
        "responses": {"200": {"description": "Password changed"}}
      }
    },
    "/auth/2fa/setup": {
      "post": {
        "tags": ["Auth - 2FA"],
        "summary": "Setup two-factor authentication",
        "security": [{"bearerAuth": []}],
        "responses": {"200": {"description": "2FA setup data"}}
      }
    },
    "/auth/2fa/verify": {
      "post": {
        "tags": ["Auth - 2FA"],
        "summary": "Verify 2FA setup",
        "security": [{"bearerAuth": []}],
        "requestBody": {"content": {"application/json": {"schema": {"type": "object", "properties": {"code": {"type": "string"}}}}}},
        "responses": {"200": {"description": "2FA verified"}}
      }
    },
    "/auth/2fa/disable": {
      "post": {
        "tags": ["Auth - 2FA"],
        "summary": "Disable 2FA",
        "security": [{"bearerAuth": []}],
        "requestBody": {"content": {"application/json": {"schema": {"type": "object", "properties": {"password": {"type": "string"}, "code": {"type": "string"}}}}}},
        "responses": {"200": {"description": "2FA disabled"}}
      }
    },
    "/promo/validate": {
      "get": {
        "tags": ["Promo"],
        "summary": "Validate a promo code (public)",
        "parameters": [{"name": "code", "in": "query", "required": true, "schema": {"type": "string"}}],
        "responses": {"200": {"description": "Promo code validity"}}
      }
    },
    "/api-keys": {
      "post": {
        "tags": ["API Keys"],
        "summary": "Create API key",
        "security": [{"bearerAuth": []}],
        "requestBody": {"content": {"application/json": {"schema": {"type": "object", "properties": {"name": {"type": "string"}, "scopes": {"type": "array", "items": {"type": "string"}}}}}}},
        "responses": {"201": {"description": "API key created"}}
      },
      "get": {
        "tags": ["API Keys"],
        "summary": "List API keys",
        "security": [{"bearerAuth": []}],
        "responses": {"200": {"description": "API key list"}}
      }
    },
    "/api-keys/{id}": {
      "delete": {
        "tags": ["API Keys"],
        "summary": "Delete API key",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}],
        "responses": {"200": {"description": "API key deleted"}}
      }
    },
    "/notifications": {
      "get": {
        "tags": ["Notifications"],
        "summary": "Get notifications",
        "security": [{"bearerAuth": []}],
        "responses": {"200": {"description": "Notification list"}}
      }
    },
    "/notifications/read": {
      "post": {
        "tags": ["Notifications"],
        "summary": "Mark notifications as read",
        "security": [{"bearerAuth": []}],
        "requestBody": {"content": {"application/json": {"schema": {"type": "object", "properties": {"notification_ids": {"type": "array", "items": {"type": "string"}}}}}}},
        "responses": {"200": {"description": "Marked as read"}}
      }
    },
    "/datasets/upload": {
      "post": {
        "tags": ["Datasets"],
        "summary": "Initiate dataset upload",
        "security": [{"bearerAuth": []}],
        "responses": {"200": {"description": "Upload initiated"}}
      }
    },
    "/datasets/{id}/complete": {
      "post": {
        "tags": ["Datasets"],
        "summary": "Complete dataset upload",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}],
        "responses": {"200": {"description": "Upload completed"}}
      }
    },
    "/datasets": {
      "get": {
        "tags": ["Datasets"],
        "summary": "List datasets",
        "security": [{"bearerAuth": []}],
        "responses": {"200": {"description": "Dataset list"}}
      }
    },
    "/datasets/{id}": {
      "get": {
        "tags": ["Datasets"],
        "summary": "Get dataset details",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}],
        "responses": {"200": {"description": "Dataset details"}}
      },
      "delete": {
        "tags": ["Datasets"],
        "summary": "Delete dataset",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}],
        "responses": {"200": {"description": "Dataset deleted"}}
      }
    },
    "/validations/create": {
      "post": {
        "tags": ["Validations"],
        "summary": "Create validation job",
        "security": [{"bearerAuth": []}],
        "requestBody": {"content": {"application/json": {"schema": {"type": "object", "properties": {"dataset_id": {"type": "string"}, "validation_type": {"type": "string"}}}}}},
        "responses": {"201": {"description": "Validation created"}}
      }
    },
    "/validations": {
      "get": {
        "tags": ["Validations"],
        "summary": "List validations",
        "security": [{"bearerAuth": []}],
        "responses": {"200": {"description": "Validation list"}}
      }
    },
    "/validations/{id}": {
      "get": {
        "tags": ["Validations"],
        "summary": "Get validation details",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}],
        "responses": {"200": {"description": "Validation details"}}
      }
    },
    "/validations/{id}/report": {
      "get": {
        "tags": ["Validations"],
        "summary": "Get validation report",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}],
        "responses": {"200": {"description": "Validation report"}}
      }
    },
    "/validations/{id}/certificate": {
      "get": {
        "tags": ["Validations"],
        "summary": "Get validation certificate",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}],
        "responses": {"200": {"description": "Certificate PDF"}}
      }
    },
    "/validations/{id}/collapse-details": {
      "get": {
        "tags": ["Validations"],
        "summary": "Get collapse details",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}],
        "responses": {"200": {"description": "Collapse details"}}
      }
    },
    "/validations/{id}/recommendations": {
      "get": {
        "tags": ["Validations"],
        "summary": "Get recommendations",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}],
        "responses": {"200": {"description": "Recommendations"}}
      }
    },
    "/warranties/{validation_id}/request": {
      "post": {
        "tags": ["Warranties"],
        "summary": "Request warranty",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "validation_id", "in": "path", "required": true, "schema": {"type": "string"}}],
        "responses": {"201": {"description": "Warranty requested"}}
      }
    },
    "/warranties": {
      "get": {
        "tags": ["Warranties"],
        "summary": "List warranties",
        "security": [{"bearerAuth": []}],
        "responses": {"200": {"description": "Warranty list"}}
      }
    },
    "/warranties/{id}": {
      "get": {
        "tags": ["Warranties"],
        "summary": "Get warranty details",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}],
        "responses": {"200": {"description": "Warranty details"}}
      }
    },
    "/warranties/{id}/claim": {
      "post": {
        "tags": ["Warranties"],
        "summary": "File warranty claim",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}],
        "responses": {"201": {"description": "Claim filed"}}
      }
    },
    "/credits/balance": {
      "get": {
        "tags": ["Credits"],
        "summary": "Get credit balance",
        "security": [{"bearerAuth": []}],
        "responses": {"200": {"description": "Credit balance"}}
      }
    },
    "/credits/packages": {
      "get": {
        "tags": ["Credits"],
        "summary": "Get credit packages",
        "security": [{"bearerAuth": []}],
        "responses": {"200": {"description": "Available packages"}}
      }
    },
    "/credits/purchase": {
      "post": {
        "tags": ["Credits"],
        "summary": "Purchase credits",
        "security": [{"bearerAuth": []}],
        "requestBody": {"content": {"application/json": {"schema": {"type": "object", "properties": {"package_id": {"type": "string"}}}}}},
        "responses": {"201": {"description": "Credits purchased"}}
      }
    },
    "/credits/history": {
      "get": {
        "tags": ["Credits"],
        "summary": "Get credit history",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "page", "in": "query", "schema": {"type": "integer"}}, {"name": "page_size", "in": "query", "schema": {"type": "integer"}}],
        "responses": {"200": {"description": "Credit history"}}
      }
    },
    "/credits/redeem": {
      "post": {
        "tags": ["Credits"],
        "summary": "Redeem promo code",
        "security": [{"bearerAuth": []}],
        "requestBody": {"content": {"application/json": {"schema": {"type": "object", "properties": {"code": {"type": "string"}}}}}},
        "responses": {"200": {"description": "Promo redeemed"}}
      }
    },
    "/analytics/usage": {
      "get": {
        "tags": ["Analytics"],
        "summary": "Get usage analytics",
        "security": [{"bearerAuth": []}],
        "responses": {"200": {"description": "Usage data"}}
      }
    },
    "/analytics/validation-history": {
      "get": {
        "tags": ["Analytics"],
        "summary": "Get validation history analytics",
        "security": [{"bearerAuth": []}],
        "responses": {"200": {"description": "Validation history data"}}
      }
    },
    "/admin/overview": {
      "get": {
        "tags": ["Admin"],
        "summary": "System overview statistics",
        "security": [{"bearerAuth": []}],
        "responses": {"200": {"description": "System overview"}}
      }
    },
    "/admin/users": {
      "get": {
        "tags": ["Admin"],
        "summary": "List all users (paginated)",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "page", "in": "query", "schema": {"type": "integer"}}, {"name": "search", "in": "query", "schema": {"type": "string"}}, {"name": "role", "in": "query", "schema": {"type": "string"}}, {"name": "status", "in": "query", "schema": {"type": "string"}}],
        "responses": {"200": {"description": "User list"}}
      }
    },
    "/admin/users/{id}": {
      "get": {
        "tags": ["Admin"],
        "summary": "Get user details",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}],
        "responses": {"200": {"description": "User details"}}
      }
    },
    "/admin/users/{id}/role": {
      "patch": {
        "tags": ["Admin"],
        "summary": "Update user role",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}],
        "requestBody": {"content": {"application/json": {"schema": {"type": "object", "properties": {"role": {"type": "string", "enum": ["admin", "developer", "support", "user"]}}}}}},
        "responses": {"200": {"description": "Role updated"}}
      }
    },
    "/admin/users/{id}/status": {
      "patch": {
        "tags": ["Admin"],
        "summary": "Update user status",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}],
        "requestBody": {"content": {"application/json": {"schema": {"type": "object", "properties": {"is_active": {"type": "boolean"}}}}}},
        "responses": {"200": {"description": "Status updated"}}
      }
    },
    "/admin/promo-codes": {
      "post": {
        "tags": ["Admin"],
        "summary": "Create promo code",
        "security": [{"bearerAuth": []}],
        "requestBody": {"content": {"application/json": {"schema": {"type": "object", "properties": {"code": {"type": "string"}, "credits_grant": {"type": "integer"}, "max_uses": {"type": "integer"}, "description": {"type": "string"}}}}}},
        "responses": {"201": {"description": "Promo code created"}}
      },
      "get": {
        "tags": ["Admin"],
        "summary": "List promo codes",
        "security": [{"bearerAuth": []}],
        "responses": {"200": {"description": "Promo code list"}}
      }
    },
    "/admin/promo-codes/{id}": {
      "patch": {
        "tags": ["Admin"],
        "summary": "Update promo code",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}],
        "requestBody": {"content": {"application/json": {"schema": {"type": "object", "properties": {"is_active": {"type": "boolean"}}}}}},
        "responses": {"200": {"description": "Promo code updated"}}
      }
    },
    "/admin/invites": {
      "post": {
        "tags": ["Admin"],
        "summary": "Create invite",
        "security": [{"bearerAuth": []}],
        "requestBody": {"content": {"application/json": {"schema": {"type": "object", "properties": {"email": {"type": "string"}, "role": {"type": "string"}}}}}},
        "responses": {"201": {"description": "Invite created"}}
      },
      "get": {
        "tags": ["Admin"],
        "summary": "List invites",
        "security": [{"bearerAuth": []}],
        "responses": {"200": {"description": "Invite list"}}
      }
    },
    "/admin/validations": {
      "get": {
        "tags": ["Admin"],
        "summary": "List all validations",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "page", "in": "query", "schema": {"type": "integer"}}, {"name": "page_size", "in": "query", "schema": {"type": "integer"}}],
        "responses": {"200": {"description": "All validations"}}
      }
    },
    "/admin/datasets": {
      "get": {
        "tags": ["Admin"],
        "summary": "List all datasets",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "page", "in": "query", "schema": {"type": "integer"}}, {"name": "page_size", "in": "query", "schema": {"type": "integer"}}],
        "responses": {"200": {"description": "All datasets"}}
      }
    },
    "/support/overview": {
      "get": {
        "tags": ["Support"],
        "summary": "Support dashboard overview",
        "security": [{"bearerAuth": []}],
        "responses": {"200": {"description": "Support overview"}}
      }
    },
    "/support/tickets": {
      "get": {
        "tags": ["Support"],
        "summary": "List support tickets",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "status", "in": "query", "schema": {"type": "string"}}, {"name": "priority", "in": "query", "schema": {"type": "string"}}, {"name": "assigned_to", "in": "query", "schema": {"type": "string"}}],
        "responses": {"200": {"description": "Ticket list"}}
      }
    },
    "/support/tickets/{id}": {
      "get": {
        "tags": ["Support"],
        "summary": "Get ticket detail with messages",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}],
        "responses": {"200": {"description": "Ticket detail"}}
      }
    },
    "/support/tickets/{id}/reply": {
      "post": {
        "tags": ["Support"],
        "summary": "Reply to ticket",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}],
        "requestBody": {"content": {"application/json": {"schema": {"type": "object", "properties": {"message": {"type": "string"}, "is_internal": {"type": "boolean"}}}}}},
        "responses": {"201": {"description": "Reply added"}}
      }
    },
    "/support/tickets/{id}/status": {
      "patch": {
        "tags": ["Support"],
        "summary": "Update ticket status",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}],
        "requestBody": {"content": {"application/json": {"schema": {"type": "object", "properties": {"status": {"type": "string", "enum": ["open", "in_progress", "waiting", "resolved", "closed"]}}}}}},
        "responses": {"200": {"description": "Status updated"}}
      }
    },
    "/support/tickets/{id}/assign": {
      "patch": {
        "tags": ["Support"],
        "summary": "Assign ticket",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}],
        "requestBody": {"content": {"application/json": {"schema": {"type": "object", "properties": {"assigned_to": {"type": "string"}}}}}},
        "responses": {"200": {"description": "Ticket assigned"}}
      }
    },
    "/support/tickets/{id}/priority": {
      "patch": {
        "tags": ["Support"],
        "summary": "Update ticket priority",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}],
        "requestBody": {"content": {"application/json": {"schema": {"type": "object", "properties": {"priority": {"type": "string", "enum": ["low", "normal", "high", "urgent"]}}}}}},
        "responses": {"200": {"description": "Priority updated"}}
      }
    },
    "/support/users/{id}": {
      "get": {
        "tags": ["Support"],
        "summary": "Get user details (read-only)",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}],
        "responses": {"200": {"description": "User details"}}
      }
    },
    "/support/users/{id}/validations": {
      "get": {
        "tags": ["Support"],
        "summary": "Get user validations",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}],
        "responses": {"200": {"description": "User validations"}}
      }
    },
    "/developer/overview": {
      "get": {
        "tags": ["Developer"],
        "summary": "Developer dashboard overview",
        "security": [{"bearerAuth": []}],
        "responses": {"200": {"description": "Dev overview"}}
      }
    },
    "/developer/services": {
      "get": {
        "tags": ["Developer"],
        "summary": "Service health status",
        "security": [{"bearerAuth": []}],
        "responses": {"200": {"description": "Services status"}}
      }
    },
    "/developer/api-docs": {
      "get": {
        "tags": ["Developer"],
        "summary": "OpenAPI specification",
        "security": [{"bearerAuth": []}],
        "responses": {"200": {"description": "OpenAPI 3.0 spec"}}
      }
    },
    "/developer/logs": {
      "get": {
        "tags": ["Developer"],
        "summary": "Recent logs/events",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "limit", "in": "query", "schema": {"type": "integer", "default": 50}}],
        "responses": {"200": {"description": "Recent log entries"}}
      }
    },
    "/developer/metrics": {
      "get": {
        "tags": ["Developer"],
        "summary": "API metrics",
        "security": [{"bearerAuth": []}],
        "responses": {"200": {"description": "Metrics data"}}
      }
    },
    "/tickets": {
      "post": {
        "tags": ["Tickets"],
        "summary": "Create support ticket",
        "security": [{"bearerAuth": []}],
        "requestBody": {"content": {"application/json": {"schema": {"type": "object", "properties": {"subject": {"type": "string"}, "message": {"type": "string"}, "category": {"type": "string"}}}}}},
        "responses": {"201": {"description": "Ticket created"}}
      },
      "get": {
        "tags": ["Tickets"],
        "summary": "List my tickets",
        "security": [{"bearerAuth": []}],
        "responses": {"200": {"description": "My tickets"}}
      }
    },
    "/tickets/{id}": {
      "get": {
        "tags": ["Tickets"],
        "summary": "Get my ticket detail",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}],
        "responses": {"200": {"description": "Ticket detail"}}
      }
    },
    "/tickets/{id}/reply": {
      "post": {
        "tags": ["Tickets"],
        "summary": "Reply to my ticket",
        "security": [{"bearerAuth": []}],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}],
        "requestBody": {"content": {"application/json": {"schema": {"type": "object", "properties": {"message": {"type": "string"}}}}}},
        "responses": {"201": {"description": "Reply added"}}
      }
    }
  },
  "components": {
    "securitySchemes": {
      "bearerAuth": {
        "type": "http",
        "scheme": "bearer",
        "bearerFormat": "JWT"
      }
    }
  }
}`

	c.Set("Content-Type", "application/json")
	return c.SendString(spec)
}

// GetRecentLogsFiber returns recent security events / audit logs
func GetRecentLogsFiber(c *fiber.Ctx) error {
	limitStr := c.Query("limit", "50")
	limit, _ := strconv.Atoi(limitStr)
	if limit < 1 || limit > 200 {
		limit = 50
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db := database.GetDB()

	rows, err := db.Query(ctx,
		`SELECT id, COALESCE(user_id, ''), event_type, success, COALESCE(ip_address, ''), COALESCE(user_agent, ''), COALESCE(location, ''), COALESCE(details::text, ''), created_at
		 FROM security_events
		 ORDER BY created_at DESC
		 LIMIT $1`, limit)
	if err != nil {
		log.Printf("Failed to query security events: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "DATABASE_ERROR", "message": "Failed to retrieve logs"},
		})
	}
	defer rows.Close()

	type LogEntry struct {
		ID        int64     `json:"id"`
		UserID    string    `json:"user_id"`
		EventType string    `json:"event_type"`
		Success   bool      `json:"success"`
		IPAddress string    `json:"ip_address"`
		UserAgent string    `json:"user_agent"`
		Location  string    `json:"location"`
		Details   string    `json:"details"`
		CreatedAt time.Time `json:"created_at"`
	}

	var logs []LogEntry
	for rows.Next() {
		var l LogEntry
		if err := rows.Scan(&l.ID, &l.UserID, &l.EventType, &l.Success, &l.IPAddress,
			&l.UserAgent, &l.Location, &l.Details, &l.CreatedAt); err != nil {
			log.Printf("Failed to scan log entry: %v", err)
			continue
		}
		logs = append(logs, l)
	}
	if logs == nil {
		logs = []LogEntry{}
	}

	return c.JSON(fiber.Map{"logs": logs, "count": len(logs)})
}

// GetMetricsFiber returns basic API metrics
func GetMetricsFiber(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db := database.GetDB()

	// Total API calls today (approximated from security_events)
	var totalEventsToday int64
	_ = db.QueryRow(ctx,
		`SELECT COUNT(*) FROM security_events WHERE created_at > NOW() - INTERVAL '24 hours'`,
	).Scan(&totalEventsToday)

	// Failed events today
	var failedEventsToday int64
	_ = db.QueryRow(ctx,
		`SELECT COUNT(*) FROM security_events WHERE success = false AND created_at > NOW() - INTERVAL '24 hours'`,
	).Scan(&failedEventsToday)

	// Validations created today
	var validationsToday int64
	_ = db.QueryRow(ctx,
		`SELECT COUNT(*) FROM validations WHERE created_at > NOW() - INTERVAL '24 hours'`,
	).Scan(&validationsToday)

	// New users today
	var newUsersToday int64
	_ = db.QueryRow(ctx,
		`SELECT COUNT(*) FROM users WHERE created_at > NOW() - INTERVAL '24 hours'`,
	).Scan(&newUsersToday)

	// Total active users (logged in within last 7 days)
	var activeUsers7d int64
	_ = db.QueryRow(ctx,
		`SELECT COUNT(*) FROM users WHERE last_login_at > NOW() - INTERVAL '7 days'`,
	).Scan(&activeUsers7d)

	return c.JSON(fiber.Map{
		"total_events_today":  totalEventsToday,
		"failed_events_today": failedEventsToday,
		"validations_today":   validationsToday,
		"new_users_today":     newUsersToday,
		"active_users_7d":     activeUsers7d,
		"measured_at":         time.Now().Format(time.RFC3339),
	})
}
