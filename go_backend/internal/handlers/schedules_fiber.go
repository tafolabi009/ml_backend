package handlers

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/tafolabi009/backend/go_backend/internal/models"
	"github.com/tafolabi009/backend/go_backend/internal/repository"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
)

// Scheduled validations ("auto-validate"): one schedule per dataset.
//   on_upload       — re-validate whenever an upload completes
//   daily / weekly  — backed by a dataset_monitor under the hood (interval
//                     24h/168h, threshold 100 so it never *alerts*; runs
//                     still produce scheduled-run notifications)

var scheduleCadences = map[string]int{
	"on_upload": 0,
	"daily":     24,
	"weekly":    168,
}

// UpsertDatasetScheduleFiber creates or replaces the dataset's schedule.
// POST /datasets/:id/schedule {"cadence":"daily","validation_type":"comprehensive"}
func UpsertDatasetScheduleFiber(c *fiber.Ctx) error {
	datasetID := c.Params("id")
	userID := c.Locals("user_id").(string)

	var req struct {
		Cadence        string `json:"cadence"`
		ValidationType string `json:"validation_type"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_REQUEST", "message": "Invalid request body"},
		})
	}
	req.Cadence = strings.ToLower(strings.TrimSpace(req.Cadence))
	if _, ok := scheduleCadences[req.Cadence]; !ok {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_CADENCE", "message": "cadence must be one of: on_upload, daily, weekly"},
		})
	}
	req.ValidationType = strings.ToLower(strings.TrimSpace(req.ValidationType))
	if req.ValidationType == "" {
		req.ValidationType = "comprehensive"
	}
	if !monitorValidationTypes[req.ValidationType] {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_VALIDATION_TYPE", "message": "Invalid validation type"},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := database.GetDB()

	dataset, err := repository.NewDatasetRepository(db).GetByID(ctx, datasetID)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{"code": "DATASET_NOT_FOUND", "message": "Dataset not found"},
		})
	}
	if dataset.UserID != userID {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": fiber.Map{"code": "FORBIDDEN", "message": "You do not have access to this dataset"},
		})
	}

	// Drop any monitor backing the previous schedule.
	var oldMonitorID *string
	_ = db.QueryRow(ctx,
		`SELECT monitor_id FROM dataset_schedules WHERE dataset_id = $1`, datasetID).Scan(&oldMonitorID)
	if oldMonitorID != nil && *oldMonitorID != "" {
		_, _ = db.Exec(ctx, `DELETE FROM dataset_monitors WHERE id = $1 AND user_id = $2`, *oldMonitorID, userID)
	}

	// Interval cadences are backed by a monitor (reuses the whole scheduler).
	var monitorID *string
	if hours := scheduleCadences[req.Cadence]; hours > 0 {
		mid := "mon_" + uuid.New().String()[:8]
		_, err = db.Exec(ctx,
			`INSERT INTO dataset_monitors (id, user_id, dataset_id, name, interval_hours, max_risk_score, validation_type, next_run_at)
			 VALUES ($1, $2, $3, $4, $5, 100, $6, NOW() + ($5 * INTERVAL '1 hour'))`,
			mid, userID, datasetID, fmt.Sprintf("Auto-validate: %s", dataset.Filename), hours, req.ValidationType)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": fiber.Map{"code": "DATABASE_ERROR", "message": "Failed to create schedule"},
			})
		}
		monitorID = &mid
	}

	_, err = db.Exec(ctx,
		`INSERT INTO dataset_schedules (dataset_id, user_id, cadence, validation_type, monitor_id, updated_at)
		 VALUES ($1, $2, $3, $4, $5, NOW())
		 ON CONFLICT (dataset_id) DO UPDATE
		 SET cadence = $3, validation_type = $4, monitor_id = $5, updated_at = NOW()`,
		datasetID, userID, req.Cadence, req.ValidationType, monitorID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "DATABASE_ERROR", "message": "Failed to save schedule"},
		})
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"dataset_id":      datasetID,
		"cadence":         req.Cadence,
		"validation_type": req.ValidationType,
		"monitor_id":      monitorID,
		"message":         "Auto-validate enabled.",
	})
}

// GetDatasetScheduleFiber returns the dataset's schedule (404 when none).
// GET /datasets/:id/schedule
func GetDatasetScheduleFiber(c *fiber.Ctx) error {
	datasetID := c.Params("id")
	userID := c.Locals("user_id").(string)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var cadence, vtype string
	var monitorID *string
	var createdAt time.Time
	err := database.GetDB().QueryRow(ctx,
		`SELECT cadence, validation_type, monitor_id, created_at
		 FROM dataset_schedules WHERE dataset_id = $1 AND user_id = $2`,
		datasetID, userID).Scan(&cadence, &vtype, &monitorID, &createdAt)
	if err != nil {
		// 200 with a null schedule: the frontend treats 404 as "feature
		// not implemented", so "no schedule set" must not 404.
		return c.JSON(fiber.Map{"schedule": nil})
	}
	return c.JSON(fiber.Map{
		"schedule": fiber.Map{
			"cadence":         cadence,
			"validation_type": vtype,
			"monitor_id":      monitorID,
			"created_at":      createdAt,
		},
		"dataset_id": datasetID,
	})
}

// DeleteDatasetScheduleFiber removes the schedule (and backing monitor).
// DELETE /datasets/:id/schedule
func DeleteDatasetScheduleFiber(c *fiber.Ctx) error {
	datasetID := c.Params("id")
	userID := c.Locals("user_id").(string)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := database.GetDB()

	var monitorID *string
	err := db.QueryRow(ctx,
		`DELETE FROM dataset_schedules WHERE dataset_id = $1 AND user_id = $2 RETURNING monitor_id`,
		datasetID, userID).Scan(&monitorID)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{"code": "NOT_FOUND", "message": "No schedule for this dataset"},
		})
	}
	if monitorID != nil && *monitorID != "" {
		_, _ = db.Exec(ctx, `DELETE FROM dataset_monitors WHERE id = $1 AND user_id = $2`, *monitorID, userID)
	}
	return c.JSON(fiber.Map{"deleted": true, "dataset_id": datasetID})
}

// triggerOnUploadValidation starts a charged validation when an upload
// completes for a dataset with an on_upload schedule. Called from the
// upload-complete handler; best-effort.
func triggerOnUploadValidation(ctx context.Context, datasetID, userID string) {
	db := database.GetDB()
	var vtype string
	err := db.QueryRow(ctx,
		`SELECT validation_type FROM dataset_schedules
		 WHERE dataset_id = $1 AND user_id = $2 AND cadence = 'on_upload'`,
		datasetID, userID).Scan(&vtype)
	if err != nil {
		return // no on_upload schedule
	}

	creditRepo := repository.NewCreditRepository(db)
	cost := int64(25)
	if cc, cerr := creditRepo.GetCreditCostByOperation(ctx, "validation_standard"); cerr == nil {
		cost = cc.CreditsRequired
	}

	validationID := "val_" + uuid.New().String()[:8]
	validation := models.Validation{
		ID:                  validationID,
		DatasetID:           datasetID,
		UserID:              userID,
		Status:              "queued",
		EstimatedCompletion: time.Now().Add(24 * time.Hour),
	}
	txn, err := creditRepo.CreateValidationCharged(ctx, &validation, vtype, "standard", cost,
		fmt.Sprintf("Auto-validation on upload for dataset %s", datasetID))
	if err != nil {
		insertNotification(ctx, userID, "scheduled_run",
			"Auto-validation skipped",
			"An upload completed but the automatic validation could not start (check your credit balance).")
		return
	}
	maybeNotifyCreditsLow(ctx, userID, txn.BalanceAfter)

	insertNotification(ctx, userID, "scheduled_run",
		"Auto-validation started",
		fmt.Sprintf("Upload complete — validation %s started automatically.", validationID))
	go processValidationAsync(validationID, datasetID, userID, vtype)
}
