package handlers

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
)

// RedeemPromoCodeRequest is the request body for promo code redemption
type RedeemPromoCodeRequest struct {
	Code string `json:"code"`
}

// RedeemPromoCodeFiber redeems a promotional code and grants credits
func RedeemPromoCodeFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)

	var req RedeemPromoCodeRequest
	if err := c.BodyParser(&req); err != nil || req.Code == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INVALID_REQUEST",
				"message": "Promo code is required",
			},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	db := database.GetDB()

	// Look up promo code
	var promoID, packageID string
	var creditsGrant int64
	var description string
	var maxUses, currentUses int
	var isActive bool

	err := db.QueryRow(ctx,
		`SELECT id, COALESCE(package_id, ''), credits_grant, COALESCE(description, ''), max_uses, current_uses, is_active
		 FROM promo_codes WHERE code = $1`, req.Code,
	).Scan(&promoID, &packageID, &creditsGrant, &description, &maxUses, &currentUses, &isActive)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INVALID_PROMO_CODE",
				"message": "This promotional code is not valid",
			},
		})
	}

	if !isActive {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "PROMO_EXPIRED",
				"message": "This promotional code has expired",
			},
		})
	}

	if maxUses > 0 && currentUses >= maxUses {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "PROMO_EXHAUSTED",
				"message": "This promotional code has reached its maximum number of uses",
			},
		})
	}

	// Check if user already redeemed this code
	var existingCount int
	db.QueryRow(ctx,
		`SELECT COUNT(*) FROM promo_redemptions WHERE promo_code_id = $1 AND user_id = $2`,
		promoID, userID,
	).Scan(&existingCount)

	if existingCount > 0 {
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "ALREADY_REDEEMED",
				"message": "You have already redeemed this promotional code",
			},
		})
	}

	// Begin transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		log.Printf("Failed to begin promo transaction: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "SERVER_ERROR", "message": "Failed to process promo code"},
		})
	}
	defer tx.Rollback(ctx)

	// Add credits to user balance
	var newBalance int64
	err = tx.QueryRow(ctx,
		`INSERT INTO credit_balances (id, user_id, balance, lifetime_purchased, lifetime_used)
		 VALUES ($1, $2, $3, $3, 0)
		 ON CONFLICT (user_id) DO UPDATE
		 SET balance = credit_balances.balance + $3,
		     lifetime_purchased = credit_balances.lifetime_purchased + $3,
		     updated_at = CURRENT_TIMESTAMP
		 RETURNING balance`,
		"cb_"+userID, userID, creditsGrant,
	).Scan(&newBalance)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "SERVER_ERROR", "message": "Failed to add credits"},
		})
	}

	// Record credit transaction
	txnID := fmt.Sprintf("ctx_%d", time.Now().UnixNano())
	refType := "promo"
	_, err = tx.Exec(ctx,
		`INSERT INTO credit_transactions (id, user_id, type, amount, balance_after, description, reference_type, reference_id)
		 VALUES ($1, $2, 'bonus', $3, $4, $5, $6, $7)`,
		txnID, userID, creditsGrant, newBalance,
		fmt.Sprintf("Promo code %s: %s", req.Code, description),
		refType, promoID,
	)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "SERVER_ERROR", "message": "Failed to record transaction"},
		})
	}

	// Record redemption
	redemptionID := "red_" + uuid.New().String()[:8]
	_, err = tx.Exec(ctx,
		`INSERT INTO promo_redemptions (id, promo_code_id, user_id, credits_granted)
		 VALUES ($1, $2, $3, $4)`,
		redemptionID, promoID, userID, creditsGrant,
	)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "SERVER_ERROR", "message": "Failed to record redemption"},
		})
	}

	// Atomically increment usage and enforce the max_uses cap inside the
	// transaction. The earlier current_uses >= max_uses check is racy on its own;
	// if two users redeem the final slot concurrently, only the UPDATE whose
	// RowsAffected==1 succeeds and the other is rolled back as exhausted.
	tag, err := tx.Exec(ctx,
		`UPDATE promo_codes
		 SET current_uses = current_uses + 1, updated_at = CURRENT_TIMESTAMP
		 WHERE id = $1 AND (max_uses <= 0 OR current_uses < max_uses)`,
		promoID,
	)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "SERVER_ERROR", "message": "Failed to update promo code"},
		})
	}
	if tag.RowsAffected() == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "PROMO_EXHAUSTED", "message": "This promotional code has reached its maximum number of uses"},
		})
	}

	if err := tx.Commit(ctx); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "SERVER_ERROR", "message": "Failed to finalize redemption"},
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"success":         true,
		"code":            req.Code,
		"credits_granted": creditsGrant,
		"new_balance":     newBalance,
		"description":     description,
		"message":         fmt.Sprintf("Successfully redeemed! %d credits added to your account.", creditsGrant),
	})
}

// ValidatePromoCodeFiber checks if a promo code is valid without redeeming it
func ValidatePromoCodeFiber(c *fiber.Ctx) error {
	code := c.Query("code")
	if code == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_REQUEST", "message": "code query parameter is required"},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db := database.GetDB()

	var credits int64
	var description string
	var isActive bool
	var maxUses, currentUses int

	err := db.QueryRow(ctx,
		`SELECT credits_grant, COALESCE(description, ''), is_active, max_uses, current_uses
		 FROM promo_codes WHERE code = $1`, code,
	).Scan(&credits, &description, &isActive, &maxUses, &currentUses)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"valid": false, "message": "Invalid promotional code",
		})
	}

	if !isActive || (maxUses > 0 && currentUses >= maxUses) {
		return c.Status(fiber.StatusOK).JSON(fiber.Map{
			"valid": false, "message": "This promotional code has expired or reached its limit",
		})
	}

	return c.JSON(fiber.Map{
		"valid":       true,
		"credits":     credits,
		"description": description,
		"message":     fmt.Sprintf("Valid! You'll receive %d credits.", credits),
	})
}

// Ensure pgxpool is used (compile check)
var _ *pgxpool.Pool
