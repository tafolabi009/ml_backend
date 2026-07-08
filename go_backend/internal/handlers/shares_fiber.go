package handlers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tafolabi009/backend/go_backend/internal/models"
	"github.com/tafolabi009/backend/go_backend/internal/repository"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
	"github.com/tafolabi009/backend/go_backend/pkg/pdfgen"
)

// Shareable read-only report links: the owner mints a token for a completed
// validation; anyone with the token can view a sanitized report (JSON or PDF)
// until it expires or is revoked. This is how reports actually get passed to
// a boss or a customer without creating accounts for them.

func shareBaseURL() string {
	if v := os.Getenv("PUBLIC_APP_URL"); v != "" {
		return v
	}
	return "https://www.synthos.dev"
}

// CreateReportShareFiber mints a share token for a completed validation.
// POST /validations/:id/share {"expires_in_hours": 168}
func CreateReportShareFiber(c *fiber.Ctx) error {
	validationID := c.Params("id")
	userID := c.Locals("user_id").(string)

	var req struct {
		ExpiresInHours int `json:"expires_in_hours"`
	}
	_ = c.BodyParser(&req) // optional body
	if req.ExpiresInHours <= 0 {
		req.ExpiresInHours = 168 // 7 days
	}
	if req.ExpiresInHours > 2160 { // 90 days
		req.ExpiresInHours = 2160
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	validationRepo := repository.NewValidationRepository(database.GetDB())
	validation, err := validationRepo.GetByID(ctx, validationID)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{"code": "NOT_FOUND", "message": "Validation not found"},
		})
	}
	if validation.UserID != userID {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": fiber.Map{"code": "FORBIDDEN", "message": "You do not have access to this validation"},
		})
	}
	if validation.Status != "completed" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "NOT_COMPLETED", "message": "Only completed validations can be shared"},
		})
	}

	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "TOKEN_ERROR", "message": "Failed to generate share token"},
		})
	}
	token := hex.EncodeToString(raw)
	expiresAt := time.Now().Add(time.Duration(req.ExpiresInHours) * time.Hour)

	_, err = database.GetDB().Exec(ctx,
		`INSERT INTO report_shares (token, user_id, validation_id, expires_at) VALUES ($1, $2, $3, $4)`,
		token, userID, validationID, expiresAt)
	if err != nil {
		log.Printf("Failed to create report share: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "DATABASE_ERROR", "message": "Failed to create share link"},
		})
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"token":      token,
		"share_url":  fmt.Sprintf("%s/shared/%s", shareBaseURL(), token),
		"api_url":    fmt.Sprintf("/api/v1/shared/reports/%s", token),
		"expires_at": expiresAt,
	})
}

// ListReportSharesFiber lists active share links for a validation (owner).
func ListReportSharesFiber(c *fiber.Ctx) error {
	validationID := c.Params("id")
	userID := c.Locals("user_id").(string)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := database.GetDB().Query(ctx,
		`SELECT token, expires_at, revoked, view_count, created_at
		 FROM report_shares
		 WHERE validation_id = $1 AND user_id = $2
		 ORDER BY created_at DESC LIMIT 50`,
		validationID, userID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "DATABASE_ERROR", "message": "Failed to list share links"},
		})
	}
	defer rows.Close()

	shares := []fiber.Map{}
	for rows.Next() {
		var token string
		var expiresAt, createdAt time.Time
		var revoked bool
		var viewCount int
		if err := rows.Scan(&token, &expiresAt, &revoked, &viewCount, &createdAt); err != nil {
			continue
		}
		shares = append(shares, fiber.Map{
			"token":      token,
			"share_url":  fmt.Sprintf("%s/shared/%s", shareBaseURL(), token),
			"expires_at": expiresAt,
			"revoked":    revoked,
			"expired":    time.Now().After(expiresAt),
			"view_count": viewCount,
			"created_at": createdAt,
		})
	}
	return c.JSON(fiber.Map{"shares": shares})
}

// RevokeReportShareFiber revokes a share link (owner).
func RevokeReportShareFiber(c *fiber.Ctx) error {
	validationID := c.Params("id")
	token := c.Params("token")
	userID := c.Locals("user_id").(string)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tag, err := database.GetDB().Exec(ctx,
		`UPDATE report_shares SET revoked = true WHERE token = $1 AND validation_id = $2 AND user_id = $3`,
		token, validationID, userID)
	if err != nil || tag.RowsAffected() == 0 {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{"code": "NOT_FOUND", "message": "Share link not found"},
		})
	}
	return c.JSON(fiber.Map{"revoked": true, "token": token})
}

// GetSharedReportFiber serves a shared report by token. PUBLIC — no auth.
// GET /shared/reports/:token[?format=pdf]
func GetSharedReportFiber(c *fiber.Ctx) error {
	token := c.Params("token")
	if len(token) < 16 || len(token) > 64 {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{"code": "NOT_FOUND", "message": "Share link not found or expired"},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := database.GetDB()

	var validationID string
	err := db.QueryRow(ctx,
		`SELECT validation_id FROM report_shares
		 WHERE token = $1 AND revoked = false AND expires_at > NOW()`,
		token).Scan(&validationID)
	if err != nil {
		// Deliberately indistinguishable: unknown, revoked and expired all 404.
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{"code": "NOT_FOUND", "message": "Share link not found or expired"},
		})
	}

	validationRepo := repository.NewValidationRepository(db)
	validation, err := validationRepo.GetByID(ctx, validationID)
	if err != nil || validation.Status != "completed" {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{"code": "NOT_FOUND", "message": "Report not available"},
		})
	}

	_, _ = db.Exec(ctx, `UPDATE report_shares SET view_count = view_count + 1 WHERE token = $1`, token)

	// Dataset display name (best effort; never expose owner identifiers).
	datasetName := "Dataset"
	datasetRepo := repository.NewDatasetRepository(db)
	if ds, derr := datasetRepo.GetByID(ctx, validation.DatasetID); derr == nil {
		datasetName = ds.Filename
	}

	results := sharedReportResults(db, validation)

	if c.Query("format") == "pdf" {
		pdfBytes, perr := pdfgen.GenerateValidationReport(validation, results)
		if perr != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": fiber.Map{"code": "PDF_GENERATION_ERROR", "message": "Failed to generate PDF report"},
			})
		}
		c.Set("Content-Type", "application/pdf")
		c.Set("Content-Disposition", fmt.Sprintf("inline; filename=synthos_report_%s.pdf", validation.ID))
		return c.Send(pdfBytes)
	}

	return c.JSON(fiber.Map{
		"validation_id": validation.ID,
		"dataset_name":  datasetName,
		"status":        validation.Status,
		"created_at":    validation.CreatedAt,
		"completed_at":  validation.CompletedAt,
		"results":       results,
		"shared":        true,
		"issuer":        "Synthos",
	})
}

// sharedReportResults assembles the sanitized result payload for shared
// views: stored ML results when available, otherwise the validation row's
// scalar fields. Contains no owner identifiers.
func sharedReportResults(db *pgxpool.Pool, validation *models.Validation) *models.ValidationResults {
	if stored, err := loadStoredResults(db, validation.ID); err == nil && stored != nil {
		return &models.ValidationResults{
			RiskScore:            stored.RiskScore,
			RiskLevel:            stored.RiskLevel,
			PredictedPerformance: stored.PredictedPerformance,
			CollapseProbability:  stored.CollapseProbability,
			Dimensions:           stored.Dimensions,
			Recommendation:       stored.Recommendation,
			WarrantyEligible:     stored.WarrantyEligible,
		}
	}

	rs := 0
	if validation.RiskScore != nil {
		rs = *validation.RiskScore
	}
	rl := "unknown"
	if validation.RiskLevel != nil {
		rl = *validation.RiskLevel
	}
	rec := ""
	if validation.Recommendation != nil {
		rec = *validation.Recommendation
	}
	we := false
	if validation.WarrantyEligible != nil {
		we = *validation.WarrantyEligible
	}
	return &models.ValidationResults{
		RiskScore:        rs,
		RiskLevel:        rl,
		Recommendation:   rec,
		WarrantyEligible: we,
	}
}
