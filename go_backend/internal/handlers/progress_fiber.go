package handlers

import (
	"context"
	"encoding/json"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/tafolabi009/backend/go_backend/internal/repository"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
)

// Live validation progress: powers the pipeline visualization on the
// validation detail page. Stage keys are the UI-facing names; internal
// current_stage values map onto them.

type pipelineStage struct {
	key      string
	label    string
	internal string // matching current_stage value ("" = virtual stage)
}

// Ordered pipeline. "queued" is virtual (creation), the rest map to the
// worker's current_stage transitions recorded in stage_history.
var pipelineStages = []pipelineStage{
	{key: "queued", label: "Queued", internal: ""},
	{key: "sampling", label: "Diversity sampling", internal: "diversity_analysis"},
	{key: "proxy_training", label: "Proxy model training", internal: "cascade_training"},
	{key: "extrapolation", label: "Collapse extrapolation", internal: "collapse_detection"},
	{key: "report", label: "Report generation", internal: "report_generation"},
}

type stageHistoryEntry struct {
	Stage string    `json:"stage"`
	At    time.Time `json:"at"`
}

// GetValidationProgressFiber returns live pipeline progress.
// GET /validations/:id/progress
func GetValidationProgressFiber(c *fiber.Ctx) error {
	validationID := c.Params("id")
	userID := c.Locals("user_id").(string)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db := database.GetDB()

	validationRepo := repository.NewValidationRepository(db)
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

	var currentStage string
	var progress float64
	var historyRaw []byte
	_ = db.QueryRow(ctx,
		`SELECT COALESCE(current_stage, ''), COALESCE(progress, 0), COALESCE(stage_history, '[]'::jsonb)
		 FROM validations WHERE id = $1`, validationID).
		Scan(&currentStage, &progress, &historyRaw)

	// Legacy rows recorded progress on a 0..1 scale; normalize to 0..100.
	if progress > 0 && progress <= 1.0 {
		progress *= 100
	}
	if validation.Status == "completed" {
		progress = 100
	}

	var history []stageHistoryEntry
	_ = json.Unmarshal(historyRaw, &history)
	startedAtFor := func(internal string) *time.Time {
		for _, h := range history {
			if h.Stage == internal {
				t := h.At
				return &t
			}
		}
		return nil
	}

	// Determine the index of the current stage within the pipeline.
	currentIdx := 0 // queued
	if validation.Status == "processing" || validation.Status == "completed" {
		for i, s := range pipelineStages {
			if s.internal != "" && s.internal == currentStage {
				currentIdx = i
			}
		}
		if validation.Status == "completed" {
			currentIdx = len(pipelineStages) // everything done
		} else if currentIdx == 0 {
			currentIdx = 1 // processing but no stage recorded yet
		}
	}
	terminal := validation.Status == "failed" || validation.Status == "cancelled"

	stages := make([]fiber.Map, 0, len(pipelineStages))
	for i, s := range pipelineStages {
		status := "pending"
		switch {
		case validation.Status == "completed" || i < currentIdx:
			status = "completed"
		case i == currentIdx && validation.Status == "processing":
			status = "running"
		case terminal && i == currentIdx && validation.Status == "failed":
			status = "failed"
		case terminal && i >= currentIdx:
			status = "skipped" // cancelled, or stages after the failure point
		}

		entry := fiber.Map{"key": s.key, "label": s.label, "status": status}

		// Timestamps: queued starts at creation; others from stage_history.
		var startedAt *time.Time
		if s.key == "queued" {
			t := validation.CreatedAt
			startedAt = &t
		} else {
			startedAt = startedAtFor(s.internal)
		}
		if startedAt != nil && status != "pending" {
			entry["started_at"] = startedAt.UTC().Format(time.RFC3339)
		}
		// A stage completes when the next one starts (or the run finishes).
		if status == "completed" {
			var completedAt *time.Time
			if i+1 < len(pipelineStages) {
				completedAt = startedAtFor(pipelineStages[i+1].internal)
			}
			if completedAt == nil && validation.CompletedAt != nil {
				completedAt = validation.CompletedAt
			}
			if completedAt != nil {
				entry["completed_at"] = completedAt.UTC().Format(time.RFC3339)
			}
		}
		stages = append(stages, entry)
	}

	// ETA: proportional estimate from elapsed processing time.
	var etaSeconds *int
	switch validation.Status {
	case "completed", "failed", "cancelled":
		zero := 0
		etaSeconds = &zero
	case "processing":
		start := validation.CreatedAt
		if validation.StartedAt != nil {
			start = *validation.StartedAt
		}
		elapsed := time.Since(start).Seconds()
		if progress >= 1 && elapsed > 0 {
			eta := int(elapsed * (100 - progress) / progress)
			if eta < 5 {
				eta = 5
			}
			if eta > 24*3600 {
				eta = 24 * 3600
			}
			etaSeconds = &eta
		}
	}

	stageKey := "queued"
	if validation.Status == "completed" {
		stageKey = "report"
	} else if currentIdx >= 1 && currentIdx < len(pipelineStages) {
		stageKey = pipelineStages[currentIdx].key
	}

	return c.JSON(fiber.Map{
		"validation_id": validationID,
		"status":        validation.Status,
		"stage":         stageKey,
		"percentage":    int(progress + 0.5),
		"eta_seconds":   etaSeconds,
		"stages":        stages,
	})
}
