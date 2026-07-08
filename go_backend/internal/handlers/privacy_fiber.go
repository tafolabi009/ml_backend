package handlers

import (
	"context"
	"encoding/json"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/tafolabi009/backend/go_backend/internal/repository"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
	"github.com/tafolabi009/backend/go_backend/pkg/storage"
)

// Privacy / PII-leakage scoring. The ML tier computes a privacy report during
// diversity analysis (PII detection, memorization risk, k-anonymity estimate)
// and writes it to S3 as a sidecar JSON next to the stratified sample:
//   samples/<dataset_id>_privacy.json
// The gateway serves it on demand — no proto change required.

var privacyStorage *storage.S3Client

// SetStorageClient wires the shared S3 client for sidecar reads (set in main).
func SetStorageClient(s3 *storage.S3Client) {
	privacyStorage = s3
}

// fetchPrivacyReport loads and parses the sidecar privacy report, if present.
func fetchPrivacyReport(ctx context.Context, datasetID string) (map[string]interface{}, bool) {
	if privacyStorage == nil || datasetID == "" {
		return nil, false
	}
	b, err := privacyStorage.GetObjectBytes(ctx, "samples/"+datasetID+"_privacy.json")
	if err != nil {
		return nil, false
	}
	var report map[string]interface{}
	if err := json.Unmarshal(b, &report); err != nil {
		return nil, false
	}
	return report, true
}

// GetValidationPrivacyFiber returns the privacy/PII analysis for the dataset
// behind a validation. GET /validations/:id/privacy
func GetValidationPrivacyFiber(c *fiber.Ctx) error {
	validationID := c.Params("id")
	userID := c.Locals("user_id").(string)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

	report, ok := fetchPrivacyReport(ctx, validation.DatasetID)
	if !ok {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "PRIVACY_NOT_AVAILABLE",
				"message": "No privacy analysis found for this dataset. It is generated during diversity analysis; re-run a validation to produce one.",
			},
		})
	}

	return c.JSON(fiber.Map{
		"validation_id": validationID,
		"dataset_id":    validation.DatasetID,
		"privacy":       report,
	})
}
