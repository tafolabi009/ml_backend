package handlers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
	"github.com/tafolabi009/backend/go_backend/pkg/webhook"
)

// validWebhookEvents defines the allowed event types for webhooks
var validWebhookEvents = map[string]bool{
	"validation.created":       true,
	"validation.completed":     true,
	"warranty.status_changed":  true,
	"credits.low":              true,
	"credits.purchased":        true,
}

// generateWebhookSecret generates a cryptographically secure secret for HMAC signing
func generateWebhookSecret() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("failed to generate webhook secret: %w", err)
	}
	return hex.EncodeToString(bytes), nil
}

// maskSecret masks a secret string, showing only first 4 and last 4 characters
func maskSecret(secret string) string {
	if len(secret) <= 8 {
		return "****"
	}
	return secret[:4] + "..." + secret[len(secret)-4:]
}

// CreateWebhookFiber creates a new webhook subscription
// POST /api/v1/webhooks
func CreateWebhookFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)

	var req struct {
		URL    string   `json:"url"`
		Events []string `json:"events"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_REQUEST", "message": "Invalid request body"},
		})
	}

	// Validate URL
	if req.URL == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "VALIDATION_ERROR", "message": "URL is required"},
		})
	}
	if err := webhook.ValidateWebhookURL(req.URL); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "VALIDATION_ERROR", "message": err.Error()},
		})
	}

	// Validate events
	if len(req.Events) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "VALIDATION_ERROR", "message": "At least one event type is required"},
		})
	}
	for _, evt := range req.Events {
		if !validWebhookEvents[evt] {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": fiber.Map{"code": "VALIDATION_ERROR", "message": fmt.Sprintf("Invalid event type: %s", evt)},
			})
		}
	}

	// Generate HMAC secret
	secret, err := generateWebhookSecret()
	if err != nil {
		log.Printf("Failed to generate webhook secret: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "INTERNAL_ERROR", "message": "Failed to generate webhook secret"},
		})
	}

	webhookID := "wh_" + uuid.New().String()[:8]

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db := database.GetDB()
	_, err = db.Exec(ctx,
		`INSERT INTO webhooks (id, user_id, url, secret, events, is_active, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, true, NOW(), NOW())`,
		webhookID, userID, req.URL, secret, req.Events,
	)
	if err != nil {
		log.Printf("Failed to create webhook: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "DATABASE_ERROR", "message": "Failed to create webhook"},
		})
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"id":         webhookID,
		"url":        req.URL,
		"secret":     secret, // Show full secret only on creation
		"events":     req.Events,
		"is_active":  true,
		"created_at": time.Now().UTC().Format(time.RFC3339),
	})
}

// ListWebhooksFiber lists all webhooks for the authenticated user
// GET /api/v1/webhooks
func ListWebhooksFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db := database.GetDB()
	rows, err := db.Query(ctx,
		`SELECT id, url, secret, events, is_active, last_triggered_at, failure_count, created_at, updated_at
		 FROM webhooks WHERE user_id = $1 ORDER BY created_at DESC`, userID)
	if err != nil {
		log.Printf("Failed to list webhooks: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "DATABASE_ERROR", "message": "Failed to list webhooks"},
		})
	}
	defer rows.Close()

	webhooks := []fiber.Map{}
	for rows.Next() {
		var id, url, secret string
		var events []string
		var isActive bool
		var lastTriggeredAt *time.Time
		var failureCount int
		var createdAt, updatedAt time.Time

		if err := rows.Scan(&id, &url, &secret, &events, &isActive, &lastTriggeredAt, &failureCount, &createdAt, &updatedAt); err != nil {
			continue
		}

		wh := fiber.Map{
			"id":            id,
			"url":           url,
			"secret":        maskSecret(secret),
			"events":        events,
			"is_active":     isActive,
			"failure_count": failureCount,
			"created_at":    createdAt.Format(time.RFC3339),
			"updated_at":    updatedAt.Format(time.RFC3339),
		}
		if lastTriggeredAt != nil {
			wh["last_triggered_at"] = lastTriggeredAt.Format(time.RFC3339)
		}
		webhooks = append(webhooks, wh)
	}

	return c.JSON(fiber.Map{"webhooks": webhooks})
}

// GetWebhookFiber returns a single webhook with recent deliveries
// GET /api/v1/webhooks/:id
func GetWebhookFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	webhookID := c.Params("id")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db := database.GetDB()

	var id, url, secret string
	var events []string
	var isActive bool
	var lastTriggeredAt *time.Time
	var failureCount int
	var createdAt, updatedAt time.Time
	var ownerID string

	err := db.QueryRow(ctx,
		`SELECT id, user_id, url, secret, events, is_active, last_triggered_at, failure_count, created_at, updated_at
		 FROM webhooks WHERE id = $1`, webhookID,
	).Scan(&id, &ownerID, &url, &secret, &events, &isActive, &lastTriggeredAt, &failureCount, &createdAt, &updatedAt)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{"code": "NOT_FOUND", "message": "Webhook not found"},
		})
	}

	if ownerID != userID {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": fiber.Map{"code": "FORBIDDEN", "message": "You do not have access to this webhook"},
		})
	}

	wh := fiber.Map{
		"id":            id,
		"url":           url,
		"secret":        maskSecret(secret),
		"events":        events,
		"is_active":     isActive,
		"failure_count": failureCount,
		"created_at":    createdAt.Format(time.RFC3339),
		"updated_at":    updatedAt.Format(time.RFC3339),
	}
	if lastTriggeredAt != nil {
		wh["last_triggered_at"] = lastTriggeredAt.Format(time.RFC3339)
	}

	// Fetch recent deliveries (last 10)
	deliveryRows, err := db.Query(ctx,
		`SELECT id, event_type, response_status, success, duration_ms, created_at
		 FROM webhook_deliveries WHERE webhook_id = $1 ORDER BY created_at DESC LIMIT 10`, webhookID)
	if err == nil {
		defer deliveryRows.Close()
		deliveries := []fiber.Map{}
		for deliveryRows.Next() {
			var dID, eventType string
			var respStatus *int
			var success bool
			var durationMs int
			var dCreatedAt time.Time
			if err := deliveryRows.Scan(&dID, &eventType, &respStatus, &success, &durationMs, &dCreatedAt); err != nil {
				continue
			}
			d := fiber.Map{
				"id":          dID,
				"event_type":  eventType,
				"success":     success,
				"duration_ms": durationMs,
				"created_at":  dCreatedAt.Format(time.RFC3339),
			}
			if respStatus != nil {
				d["response_status"] = *respStatus
			}
			deliveries = append(deliveries, d)
		}
		wh["recent_deliveries"] = deliveries
	}

	return c.JSON(wh)
}

// UpdateWebhookFiber updates a webhook's configuration
// PATCH /api/v1/webhooks/:id
func UpdateWebhookFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	webhookID := c.Params("id")

	var req struct {
		URL      *string  `json:"url"`
		Events   []string `json:"events"`
		IsActive *bool    `json:"is_active"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_REQUEST", "message": "Invalid request body"},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db := database.GetDB()

	// Verify ownership
	var ownerID string
	err := db.QueryRow(ctx, `SELECT user_id FROM webhooks WHERE id = $1`, webhookID).Scan(&ownerID)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{"code": "NOT_FOUND", "message": "Webhook not found"},
		})
	}
	if ownerID != userID {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": fiber.Map{"code": "FORBIDDEN", "message": "You do not have access to this webhook"},
		})
	}

	// Validate URL if provided
	if req.URL != nil {
		if err := webhook.ValidateWebhookURL(*req.URL); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": fiber.Map{"code": "VALIDATION_ERROR", "message": err.Error()},
			})
		}
	}

	// Validate events if provided
	if len(req.Events) > 0 {
		for _, evt := range req.Events {
			if !validWebhookEvents[evt] {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
					"error": fiber.Map{"code": "VALIDATION_ERROR", "message": fmt.Sprintf("Invalid event type: %s", evt)},
				})
			}
		}
	}

	// Build dynamic update
	if req.URL != nil {
		db.Exec(ctx, `UPDATE webhooks SET url = $1, updated_at = NOW() WHERE id = $2`, *req.URL, webhookID)
	}
	if len(req.Events) > 0 {
		db.Exec(ctx, `UPDATE webhooks SET events = $1, updated_at = NOW() WHERE id = $2`, req.Events, webhookID)
	}
	if req.IsActive != nil {
		db.Exec(ctx, `UPDATE webhooks SET is_active = $1, updated_at = NOW() WHERE id = $2`, *req.IsActive, webhookID)
	}

	return c.JSON(fiber.Map{"message": "Webhook updated successfully"})
}

// DeleteWebhookFiber deletes a webhook
// DELETE /api/v1/webhooks/:id
func DeleteWebhookFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	webhookID := c.Params("id")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db := database.GetDB()

	// Verify ownership
	var ownerID string
	err := db.QueryRow(ctx, `SELECT user_id FROM webhooks WHERE id = $1`, webhookID).Scan(&ownerID)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{"code": "NOT_FOUND", "message": "Webhook not found"},
		})
	}
	if ownerID != userID {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": fiber.Map{"code": "FORBIDDEN", "message": "You do not have access to this webhook"},
		})
	}

	_, err = db.Exec(ctx, `DELETE FROM webhooks WHERE id = $1`, webhookID)
	if err != nil {
		log.Printf("Failed to delete webhook: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "DATABASE_ERROR", "message": "Failed to delete webhook"},
		})
	}

	return c.JSON(fiber.Map{"message": "Webhook deleted successfully"})
}

// ListWebhookDeliveriesFiber returns paginated webhook delivery logs
// GET /api/v1/webhooks/:id/deliveries
func ListWebhookDeliveriesFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	webhookID := c.Params("id")

	page, _ := strconv.Atoi(c.Query("page", "1"))
	if page < 1 {
		page = 1
	}
	pageSize, _ := strconv.Atoi(c.Query("page_size", "20"))
	if pageSize < 1 || pageSize > 100 {
		pageSize = 20
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db := database.GetDB()

	// Verify ownership
	var ownerID string
	err := db.QueryRow(ctx, `SELECT user_id FROM webhooks WHERE id = $1`, webhookID).Scan(&ownerID)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{"code": "NOT_FOUND", "message": "Webhook not found"},
		})
	}
	if ownerID != userID {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": fiber.Map{"code": "FORBIDDEN", "message": "You do not have access to this webhook"},
		})
	}

	// Count total
	var totalCount int
	db.QueryRow(ctx, `SELECT COUNT(*) FROM webhook_deliveries WHERE webhook_id = $1`, webhookID).Scan(&totalCount)

	offset := (page - 1) * pageSize
	rows, err := db.Query(ctx,
		`SELECT id, event_type, payload, response_status, response_body, success, duration_ms, created_at
		 FROM webhook_deliveries WHERE webhook_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3`,
		webhookID, pageSize, offset)
	if err != nil {
		log.Printf("Failed to list webhook deliveries: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "DATABASE_ERROR", "message": "Failed to list deliveries"},
		})
	}
	defer rows.Close()

	deliveries := []fiber.Map{}
	for rows.Next() {
		var dID, eventType, responseBody string
		var payload []byte
		var respStatus *int
		var success bool
		var durationMs int
		var createdAt time.Time

		if err := rows.Scan(&dID, &eventType, &payload, &respStatus, &responseBody, &success, &durationMs, &createdAt); err != nil {
			continue
		}

		d := fiber.Map{
			"id":            dID,
			"event_type":    eventType,
			"payload":       string(payload),
			"response_body": responseBody,
			"success":       success,
			"duration_ms":   durationMs,
			"created_at":    createdAt.Format(time.RFC3339),
		}
		if respStatus != nil {
			d["response_status"] = *respStatus
		}
		deliveries = append(deliveries, d)
	}

	totalPages := (totalCount + pageSize - 1) / pageSize

	return c.JSON(fiber.Map{
		"deliveries": deliveries,
		"pagination": fiber.Map{
			"page":        page,
			"page_size":   pageSize,
			"total_count": totalCount,
			"total_pages": totalPages,
		},
	})
}
