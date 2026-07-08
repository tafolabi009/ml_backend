package handlers

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
)

// Per-user quotas protecting the ML tier and the bill. Defaults come from env
// (QUOTA_MAX_DATASET_BYTES, QUOTA_MAX_VALIDATIONS_PER_DAY); a row in
// user_quotas overrides them for a specific user (set by an admin).

const (
	defaultMaxDatasetBytes      = int64(5) << 30 // 5 GiB
	defaultMaxValidationsPerDay = 100
)

type userQuota struct {
	MaxDatasetBytes      int64 `json:"max_dataset_bytes"`
	MaxValidationsPerDay int   `json:"max_validations_per_day"`
}

func envInt64(key string, fallback int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

// getUserQuota resolves the effective quota for a user (override row → env → defaults).
func getUserQuota(ctx context.Context, userID string) userQuota {
	q := userQuota{
		MaxDatasetBytes:      envInt64("QUOTA_MAX_DATASET_BYTES", defaultMaxDatasetBytes),
		MaxValidationsPerDay: int(envInt64("QUOTA_MAX_VALIDATIONS_PER_DAY", defaultMaxValidationsPerDay)),
	}
	var maxBytes *int64
	var maxPerDay *int
	err := database.GetDB().QueryRow(ctx,
		`SELECT max_dataset_bytes, max_validations_per_day FROM user_quotas WHERE user_id = $1`,
		userID).Scan(&maxBytes, &maxPerDay)
	if err == nil {
		if maxBytes != nil && *maxBytes > 0 {
			q.MaxDatasetBytes = *maxBytes
		}
		if maxPerDay != nil && *maxPerDay > 0 {
			q.MaxValidationsPerDay = *maxPerDay
		}
	}
	return q
}

// countValidationsToday returns how many validations the user created in the
// last 24 hours (cancelled ones don't count against the quota).
func countValidationsToday(ctx context.Context, userID string) (int, error) {
	var n int
	err := database.GetDB().QueryRow(ctx,
		`SELECT COUNT(*) FROM validations
		 WHERE user_id = $1 AND created_at > NOW() - INTERVAL '24 hours' AND status <> 'cancelled'`,
		userID).Scan(&n)
	return n, err
}

// GetMyQuotaFiber returns the caller's effective quota and current usage.
func GetMyQuotaFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	q := getUserQuota(ctx, userID)
	used, err := countValidationsToday(ctx, userID)
	if err != nil {
		log.Printf("quota usage count failed: %v", err)
	}
	return c.JSON(fiber.Map{
		"max_dataset_bytes":       q.MaxDatasetBytes,
		"max_validations_per_day": q.MaxValidationsPerDay,
		"validations_used_24h":    used,
	})
}

// GetUserQuotaAdminFiber returns another user's effective quota (admin).
func GetUserQuotaAdminFiber(c *fiber.Ctx) error {
	targetID := c.Params("id")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	q := getUserQuota(ctx, targetID)
	used, _ := countValidationsToday(ctx, targetID)
	return c.JSON(fiber.Map{
		"user_id":                 targetID,
		"max_dataset_bytes":       q.MaxDatasetBytes,
		"max_validations_per_day": q.MaxValidationsPerDay,
		"validations_used_24h":    used,
	})
}

// UpdateUserQuotaAdminFiber sets per-user overrides (admin). Zero/omitted
// fields fall back to the env defaults.
func UpdateUserQuotaAdminFiber(c *fiber.Ctx) error {
	targetID := c.Params("id")

	var req struct {
		MaxDatasetBytes      *int64 `json:"max_dataset_bytes"`
		MaxValidationsPerDay *int   `json:"max_validations_per_day"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_REQUEST", "message": "Invalid request body"},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := database.GetDB().Exec(ctx,
		`INSERT INTO user_quotas (user_id, max_dataset_bytes, max_validations_per_day, updated_at)
		 VALUES ($1, $2, $3, NOW())
		 ON CONFLICT (user_id) DO UPDATE
		 SET max_dataset_bytes = $2, max_validations_per_day = $3, updated_at = NOW()`,
		targetID, req.MaxDatasetBytes, req.MaxValidationsPerDay)
	if err != nil {
		log.Printf("quota update failed for %s: %v", targetID, err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "DATABASE_ERROR", "message": "Failed to update quota"},
		})
	}

	q := getUserQuota(ctx, targetID)
	return c.JSON(fiber.Map{
		"user_id":                 targetID,
		"max_dataset_bytes":       q.MaxDatasetBytes,
		"max_validations_per_day": q.MaxValidationsPerDay,
	})
}
