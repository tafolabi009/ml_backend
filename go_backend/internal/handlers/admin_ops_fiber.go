package handlers

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/tafolabi009/backend/go_backend/internal/auth"
	"github.com/tafolabi009/backend/go_backend/internal/repository"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
)

// ---------------------------------------------------------------------------
// Task 1: editable validation names
// ---------------------------------------------------------------------------

// ValidateRenameName applies the rename rules: trim, 1-120 chars, non-blank.
func ValidateRenameName(raw string) (string, bool) {
	name := strings.TrimSpace(raw)
	if name == "" || len(name) > 120 {
		return "", false
	}
	return name, true
}

// RenameValidationFiber — PATCH /validations/:id {"name": string}
func RenameValidationFiber(c *fiber.Ctx) error {
	validationID := c.Params("id")
	userID := c.Locals("user_id").(string)

	var req struct {
		Name string `json:"name"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_REQUEST", "message": "Invalid request body"},
		})
	}
	name, ok := ValidateRenameName(req.Name)
	if !ok {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_NAME", "message": "name must be 1-120 characters and not blank"},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := database.GetDB()

	// Owner-only (same authz pattern as other validation mutations).
	tag, err := db.Exec(ctx,
		`UPDATE validations SET name = $3, updated_at = NOW() WHERE id = $1 AND user_id = $2`,
		validationID, userID, name)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "DATABASE_ERROR", "message": "Failed to rename validation"},
		})
	}
	if tag.RowsAffected() == 0 {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{"code": "NOT_FOUND", "message": "Validation not found"},
		})
	}

	// Audit entry (security_events is the existing audit sink).
	details, _ := json.Marshal(fiber.Map{"validation_id": validationID, "new_name": name})
	_, _ = db.Exec(ctx,
		`INSERT INTO security_events (user_id, event_type, success, ip_address, details)
		 VALUES ($1, 'validation_renamed', true, $2, $3)`, userID, c.IP(), details)

	// Return the updated validation object (existing detail handler shape).
	return GetValidationFiber(c)
}

// validationDisplayName returns the custom name when set (used by reports,
// certificates and datasheets so renames propagate everywhere).
func validationDisplayName(ctx context.Context, validationID string) string {
	var name *string
	_ = database.GetDB().QueryRow(ctx,
		`SELECT name FROM validations WHERE id = $1`, validationID).Scan(&name)
	if name != nil {
		return strings.TrimSpace(*name)
	}
	return ""
}

// ---------------------------------------------------------------------------
// Task 2: Paddle billing webhook
// ---------------------------------------------------------------------------

// VerifyPaddleSignature checks a Paddle-Signature header ("ts=...;h1=...")
// against the raw body using HMAC-SHA256 over "<ts>:<body>". Rejects stale
// timestamps (>5 min skew) to limit replay.
func VerifyPaddleSignature(header string, body []byte, secret string, now time.Time) bool {
	if header == "" || secret == "" {
		return false
	}
	var ts, h1 string
	for _, part := range strings.Split(header, ";") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "ts":
			ts = kv[1]
		case "h1":
			h1 = kv[1]
		}
	}
	if ts == "" || h1 == "" {
		return false
	}
	var tsUnix int64
	if _, err := fmt.Sscanf(ts, "%d", &tsUnix); err != nil {
		return false
	}
	if d := now.Unix() - tsUnix; d > 300 || d < -300 {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(ts))
	mac.Write([]byte(":"))
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(expected), []byte(h1))
}

// PaddleWebhookFiber — POST /webhooks/paddle (public, signature-verified).
// Handles transaction.completed (credit provisioning + receipt_url) and
// transaction.refunded (idempotent claw-back).
func PaddleWebhookFiber(c *fiber.Ctx) error {
	secret := strings.TrimSpace(os.Getenv("PADDLE_WEBHOOK_SECRET"))
	body := c.Body()
	if !VerifyPaddleSignature(c.Get("Paddle-Signature"), body, secret, time.Now()) {
		// Never log payloads: they can contain customer PII.
		log.Printf("paddle webhook: signature verification failed")
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_SIGNATURE", "message": "Signature verification failed"},
		})
	}

	var event struct {
		EventType string `json:"event_type"`
		Data      struct {
			ID         string `json:"id"`
			Status     string `json:"status"`
			CustomData struct {
				UserID    string `json:"user_id"`
				PackageID string `json:"package_id"`
			} `json:"custom_data"`
			Items []struct {
				Price struct {
					ID string `json:"id"`
				} `json:"price"`
			} `json:"items"`
			ReceiptURL string `json:"receipt_url"`
			Checkout   struct {
				URL string `json:"url"`
			} `json:"checkout"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &event); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_PAYLOAD", "message": "Malformed webhook payload"},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db := database.GetDB()

	switch event.EventType {
	case "transaction.completed":
		userID := event.Data.CustomData.UserID
		if userID == "" {
			log.Printf("paddle webhook: transaction %s has no custom_data.user_id; acknowledged without provisioning", event.Data.ID)
			return c.JSON(fiber.Map{"received": true, "provisioned": false})
		}

		// Idempotency: at most one grant per Paddle transaction id (also
		// DB-enforced by the uq_paddle_txn partial unique index).
		var exists bool
		_ = db.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM credit_transactions WHERE reference_type = 'paddle' AND reference_id = $1)`,
			event.Data.ID).Scan(&exists)
		if exists {
			return c.JSON(fiber.Map{"received": true, "provisioned": false, "duplicate": true})
		}

		// Resolve the package: explicit custom_data first, then price mapping.
		creditRepo := repository.NewCreditRepository(db)
		pkgID := event.Data.CustomData.PackageID
		if pkgID == "" && len(event.Data.Items) > 0 {
			_ = db.QueryRow(ctx,
				`SELECT id FROM credit_packages WHERE paddle_price_id = $1 AND is_active = true`,
				event.Data.Items[0].Price.ID).Scan(&pkgID)
		}
		if pkgID == "" {
			log.Printf("paddle webhook: transaction %s has no resolvable package; acknowledged", event.Data.ID)
			return c.JSON(fiber.Map{"received": true, "provisioned": false})
		}
		pkg, perr := creditRepo.GetPackageByID(ctx, pkgID)
		if perr != nil {
			log.Printf("paddle webhook: unknown package %s for transaction %s", pkgID, event.Data.ID)
			return c.JSON(fiber.Map{"received": true, "provisioned": false})
		}

		refType := "paddle"
		paddleTxnID := event.Data.ID
		total := pkg.Credits + pkg.BonusCredits
		txn, aerr := creditRepo.AddCredits(ctx, userID, total, "purchase",
			fmt.Sprintf("Paddle purchase: %s package", pkg.Name), &refType, &paddleTxnID)
		if aerr != nil {
			// Unique-index violation = concurrent duplicate delivery: acknowledge.
			log.Printf("paddle webhook: provisioning skipped for %s: %v", paddleTxnID, aerr)
			return c.JSON(fiber.Map{"received": true, "provisioned": false, "duplicate": true})
		}

		// Record the hosted receipt for GET /credits/history.
		receipt := event.Data.ReceiptURL
		if receipt == "" {
			receipt = event.Data.Checkout.URL
		}
		if receipt != "" {
			meta, _ := json.Marshal(fiber.Map{"receipt_url": receipt, "paddle_transaction_id": paddleTxnID})
			_, _ = db.Exec(ctx, `UPDATE credit_transactions SET metadata = $2::jsonb WHERE id = $1`, txn.ID, meta)
		}

		insertNotification(ctx, userID, "credits_purchased", "Credits added",
			fmt.Sprintf("%d credits were added to your balance (Paddle).", total))
		return c.JSON(fiber.Map{"received": true, "provisioned": true})

	case "transaction.refunded", "adjustment.created":
		userID := event.Data.CustomData.UserID
		refundRef := "paddle_refund_" + event.Data.ID
		if userID == "" {
			return c.JSON(fiber.Map{"received": true})
		}
		var exists bool
		_ = db.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM credit_transactions WHERE reference_type = 'paddle' AND reference_id = $1)`,
			refundRef).Scan(&exists)
		if exists {
			return c.JSON(fiber.Map{"received": true, "duplicate": true})
		}
		// Claw back the original grant if we can find it.
		var granted int64
		_ = db.QueryRow(ctx,
			`SELECT amount FROM credit_transactions WHERE reference_type = 'paddle' AND reference_id = $1 AND type = 'purchase'`,
			event.Data.ID).Scan(&granted)
		if granted > 0 {
			refType := "paddle"
			creditRepo := repository.NewCreditRepository(db)
			_, _ = creditRepo.AddCredits(ctx, userID, -granted, "adjustment",
				fmt.Sprintf("Paddle refund/chargeback for transaction %s", event.Data.ID), &refType, &refundRef)
			insertNotification(ctx, userID, "credits_adjusted", "Credits removed",
				fmt.Sprintf("%d credits were removed following a Paddle refund.", granted))
		}
		return c.JSON(fiber.Map{"received": true})
	}

	// Unhandled event types are acknowledged so Paddle stops retrying.
	return c.JSON(fiber.Map{"received": true, "ignored": event.EventType})
}

// ---------------------------------------------------------------------------
// Task 3: admin metrics + impersonation
// ---------------------------------------------------------------------------

// MetricsGranularityTrunc maps API granularity to a date_trunc unit.
func MetricsGranularityTrunc(g string) (string, bool) {
	switch g {
	case "", "day":
		return "day", true
	case "week":
		return "week", true
	case "month":
		return "month", true
	}
	return "", false
}

// GetAdminMetricsFiber — GET /admin/metrics?metric=&from=&to=&granularity=
func GetAdminMetricsFiber(c *fiber.Ctx) error {
	metric := c.Query("metric", "signups")
	granularity := c.Query("granularity", "day")
	trunc, ok := MetricsGranularityTrunc(granularity)
	if !ok {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_GRANULARITY", "message": "granularity must be day, week or month"},
		})
	}

	parseTime := func(q string, fallback time.Time) time.Time {
		if q == "" {
			return fallback
		}
		for _, layout := range []string{time.RFC3339, "2006-01-02"} {
			if t, err := time.Parse(layout, q); err == nil {
				return t
			}
		}
		return fallback
	}
	to := parseTime(c.Query("to"), time.Now())
	from := parseTime(c.Query("from"), to.AddDate(0, 0, -30))

	var query string
	switch metric {
	case "signups":
		query = `SELECT date_trunc('` + trunc + `', created_at) AS bucket, COUNT(*)::float8
		         FROM users WHERE created_at >= $1 AND created_at <= $2 GROUP BY 1 ORDER BY 1`
	case "validations":
		query = `SELECT date_trunc('` + trunc + `', created_at) AS bucket, COUNT(*)::float8
		         FROM validations WHERE created_at >= $1 AND created_at <= $2 GROUP BY 1 ORDER BY 1`
	case "revenue":
		// Cents from Paddle-provisioned purchases joined to package prices.
		query = `SELECT date_trunc('` + trunc + `', ct.created_at) AS bucket,
		                COALESCE(SUM(cp.price_cents), 0)::float8
		         FROM credit_transactions ct
		         JOIN credit_packages cp ON cp.id = ct.reference_id OR cp.paddle_price_id = ct.reference_id
		         WHERE ct.type = 'purchase' AND ct.created_at >= $1 AND ct.created_at <= $2
		         GROUP BY 1 ORDER BY 1`
	default:
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_METRIC", "message": "metric must be signups, validations or revenue"},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rows, err := database.GetDB().Query(ctx, query, from, to)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "DATABASE_ERROR", "message": "Failed to compute metrics"},
		})
	}
	defer rows.Close()

	series := []fiber.Map{}
	for rows.Next() {
		var bucket time.Time
		var value float64
		if rows.Scan(&bucket, &value) == nil {
			series = append(series, fiber.Map{"bucket": bucket.UTC().Format(time.RFC3339), "value": value})
		}
	}
	return c.JSON(fiber.Map{"metric": metric, "granularity": trunc, "series": series})
}

// ImpersonateUserFiber — POST /admin/impersonate {"user_id"} (admin-only route).
func ImpersonateUserFiber(c *fiber.Ctx) error {
	adminID := c.Locals("user_id").(string)

	var req struct {
		UserID string `json:"user_id"`
	}
	if err := c.BodyParser(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_REQUEST", "message": "user_id is required"},
		})
	}
	if req.UserID == adminID {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_TARGET", "message": "Cannot impersonate yourself"},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := database.GetDB()

	var email string
	var role *string
	if err := db.QueryRow(ctx,
		`SELECT email, role FROM users WHERE id = $1 AND is_active = true`, req.UserID).
		Scan(&email, &role); err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{"code": "NOT_FOUND", "message": "User not found or inactive"},
		})
	}
	targetRole := "user"
	if role != nil && *role != "" {
		targetRole = *role
	}
	if targetRole == "admin" {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": fiber.Map{"code": "FORBIDDEN", "message": "Admins cannot be impersonated"},
		})
	}

	const impersonationTTL = 15 * time.Minute
	token, err := auth.GenerateImpersonationToken(req.UserID, email, targetRole, adminID, impersonationTTL)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "TOKEN_ERROR", "message": "Failed to mint impersonation token"},
		})
	}

	details, _ := json.Marshal(fiber.Map{"impersonator_id": adminID, "target_user_id": req.UserID, "ttl_minutes": 15})
	_, _ = db.Exec(ctx,
		`INSERT INTO security_events (user_id, event_type, success, ip_address, details)
		 VALUES ($1, 'impersonation_started', true, $2, $3)`, req.UserID, c.IP(), details)

	return c.JSON(fiber.Map{
		"token":           token,
		"expires_at":      time.Now().Add(impersonationTTL).UTC().Format(time.RFC3339),
		"impersonator_id": adminID,
	})
}
