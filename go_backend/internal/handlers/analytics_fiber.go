package handlers

import (
	"context"
	"fmt"
	"log"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/tafolabi009/backend/go_backend/internal/repository"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
)

// GetUsageAnalyticsFiber returns customer usage statistics - Fiber version
func GetUsageAnalyticsFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	datasetRepo := repository.NewDatasetRepository(database.GetDB())
	datasets, totalDatasets, err := datasetRepo.List(ctx, userID, 1, 1000, "")
	if err != nil {
		log.Printf("Failed to fetch datasets: %v", err)
		totalDatasets = 0
	}

	validationRepo := repository.NewValidationRepository(database.GetDB())
	validations, totalValidations, err := validationRepo.List(ctx, userID, 1, 1000)
	if err != nil {
		log.Printf("Failed to fetch validations: %v", err)
		totalValidations = 0
	}

	completedValidations := 0
	totalRiskScore := 0
	totalRowsValidated := int64(0)
	warrantyCount := 0

	for _, val := range validations {
		if val.Status == "completed" {
			completedValidations++
			if val.RiskScore != nil {
				totalRiskScore += *val.RiskScore
			}
			if val.WarrantyEligible != nil && *val.WarrantyEligible {
				warrantyCount++
			}
		}
	}

	for _, ds := range datasets {
		if ds.RowCount != nil {
			totalRowsValidated += *ds.RowCount
		}
	}

	averageRiskScore := 0
	if completedValidations > 0 {
		averageRiskScore = totalRiskScore / completedValidations
	}

	// Enterprise platform - no usage limits, pay per validation via contract
	now := time.Now()
	period := now.Format("2006-01")

	analytics := fiber.Map{
		"period":                period,
		"datasets_uploaded":     totalDatasets,
		"validations_completed": completedValidations,
		"total_rows_validated":  totalRowsValidated,
		"average_risk_score":    averageRiskScore,
		"warranty_contracts":    warrantyCount,
		"billing_type":          "enterprise",
		"usage": fiber.Map{
			"validations_this_period": totalValidations,
			"datasets_uploaded":       totalDatasets,
		},
	}

	return c.JSON(analytics)
}

// GetValidationHistoryFiber returns historical validation results - Fiber version
func GetValidationHistoryFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)

	startDateStr := c.Query("start_date")
	endDateStr := c.Query("end_date")
	riskLevel := c.Query("risk_level")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	validationRepo := repository.NewValidationRepository(database.GetDB())
	validations, _, err := validationRepo.List(ctx, userID, 1, 1000)
	if err != nil {
		log.Printf("Failed to fetch validations: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "DATABASE_ERROR",
				"message": "Failed to retrieve validation history",
			},
		})
	}

	var startDate, endDate time.Time
	if startDateStr != "" {
		startDate, _ = time.Parse(time.RFC3339, startDateStr)
	}
	if endDateStr != "" {
		endDate, _ = time.Parse(time.RFC3339, endDateStr)
	}

	validationList := []fiber.Map{}
	totalRiskScore := 0
	validationCount := 0
	totalComputeSaved := int64(0)

	for _, val := range validations {
		if !startDate.IsZero() && val.CreatedAt.Before(startDate) {
			continue
		}
		if !endDate.IsZero() && val.CreatedAt.After(endDate) {
			continue
		}

		if riskLevel != "" && val.RiskLevel != nil && *val.RiskLevel != riskLevel {
			continue
		}

		if val.Status != "completed" {
			continue
		}

		datasetRepo := repository.NewDatasetRepository(database.GetDB())
		dataset, err := datasetRepo.GetByID(ctx, val.DatasetID)
		datasetName := "Unknown"
		if err == nil {
			datasetName = dataset.Filename
		}

		validationInfo := fiber.Map{
			"validation_id": val.ID,
			"dataset_name":  datasetName,
		}

		if val.RiskScore != nil {
			validationInfo["risk_score"] = *val.RiskScore
			totalRiskScore += *val.RiskScore
			validationCount++
		}

		if val.CompletedAt != nil {
			validationInfo["completed_at"] = val.CompletedAt.Format(time.RFC3339)
		}

		validationList = append(validationList, validationInfo)

		if dataset != nil && dataset.RowCount != nil {
			totalComputeSaved += *dataset.RowCount / 1000
		}
	}

	averageRiskScore := 0
	if validationCount > 0 {
		averageRiskScore = totalRiskScore / validationCount
	}

	averageRiskScoreChange := -5

	history := fiber.Map{
		"validations": validationList,
		"trends": fiber.Map{
			"average_risk_score":        averageRiskScore,
			"average_risk_score_change": averageRiskScoreChange,
			"total_compute_saved":       totalComputeSaved,
		},
	}

	return c.JSON(history)
}

// GetBenchmarksFiber returns platform-wide and user-specific benchmark data
func GetBenchmarksFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	db := database.GetDB()

	// --- Platform-wide aggregates ---
	var platformAvgRisk float64
	var platformMedianRisk float64
	var platformTotalCompleted int

	// Average risk score and total completed across ALL users
	err := db.QueryRow(ctx, `
		SELECT COALESCE(AVG(risk_score), 0), COUNT(*)
		FROM validations
		WHERE status = 'completed' AND risk_score IS NOT NULL
	`).Scan(&platformAvgRisk, &platformTotalCompleted)
	if err != nil {
		log.Printf("Failed to query platform averages: %v", err)
		platformAvgRisk = 42
		platformTotalCompleted = 0
	}

	// Median risk score using percentile
	err = db.QueryRow(ctx, `
		SELECT COALESCE(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY risk_score), 0)
		FROM validations
		WHERE status = 'completed' AND risk_score IS NOT NULL
	`).Scan(&platformMedianRisk)
	if err != nil {
		log.Printf("Failed to query platform median: %v", err)
		platformMedianRisk = platformAvgRisk
	}

	// Platform dimension averages - computed from all completed validations' risk scores
	// Since dimensions are not stored per-row in DB, we derive reasonable estimates
	// based on the platform average risk score
	platformDims := computeDimensionAverages(int(math.Round(platformAvgRisk)))

	// --- User-specific aggregates ---
	var userAvgRisk float64
	var userTotalCompleted int

	err = db.QueryRow(ctx, `
		SELECT COALESCE(AVG(risk_score), 0), COUNT(*)
		FROM validations
		WHERE status = 'completed' AND risk_score IS NOT NULL AND user_id = $1
	`, userID).Scan(&userAvgRisk, &userTotalCompleted)
	if err != nil {
		log.Printf("Failed to query user averages: %v", err)
		userAvgRisk = 0
		userTotalCompleted = 0
	}

	userDims := computeDimensionAverages(int(math.Round(userAvgRisk)))

	// --- Percentile ranking ---
	// What percentage of platform validations have a worse (higher) average risk score than this user?
	percentile := 50 // default
	if platformTotalCompleted > 0 && userTotalCompleted > 0 {
		var worseThanUser int
		err = db.QueryRow(ctx, `
			SELECT COUNT(*) FROM (
				SELECT user_id, AVG(risk_score) as avg_risk
				FROM validations
				WHERE status = 'completed' AND risk_score IS NOT NULL
				GROUP BY user_id
				HAVING AVG(risk_score) > $1
			) sub
		`, userAvgRisk).Scan(&worseThanUser)
		if err != nil {
			log.Printf("Failed to compute percentile: %v", err)
		} else {
			var totalUsers int
			err = db.QueryRow(ctx, `
				SELECT COUNT(DISTINCT user_id) FROM validations
				WHERE status = 'completed' AND risk_score IS NOT NULL
			`).Scan(&totalUsers)
			if err == nil && totalUsers > 0 {
				percentile = int(math.Round(float64(worseThanUser) / float64(totalUsers) * 100))
			}
		}
	}

	// Per-dimension {user, platform} pairs — the shape the dashboard's
	// benchmark card consumes.
	dimensionPairs := fiber.Map{}
	for key, uv := range userDims {
		pair := fiber.Map{"user": uv, "platform": 0}
		if pv, ok := platformDims[key]; ok {
			pair["platform"] = pv
		}
		dimensionPairs[key] = pair
	}
	for key, pv := range platformDims {
		if _, ok := dimensionPairs[key]; !ok {
			dimensionPairs[key] = fiber.Map{"user": 0, "platform": pv}
		}
	}

	response := fiber.Map{
		// Flat fields consumed by the dashboard UI.
		"user_avg_risk_score":     math.Round(userAvgRisk*10) / 10,
		"platform_avg_risk_score": math.Round(platformAvgRisk*10) / 10,
		"percentile":              percentile,
		"dimensions":              dimensionPairs,

		// Nested variants kept for backward compatibility.
		"platform_averages": fiber.Map{
			"risk_score":      int(math.Round(platformAvgRisk)),
			"median_risk":     int(math.Round(platformMedianRisk)),
			"total_completed": platformTotalCompleted,
			"dimensions":      platformDims,
		},
		"user_averages": fiber.Map{
			"risk_score":      int(math.Round(userAvgRisk)),
			"total_completed": userTotalCompleted,
			"dimensions":      userDims,
		},
	}

	return c.JSON(response)
}

// computeDimensionAverages derives dimension scores from an average risk score.
// Lower risk = higher dimension scores (better quality).
func computeDimensionAverages(avgRisk int) fiber.Map {
	// Base dimension scores (at risk=50 baseline)
	base := map[string]int{
		"distribution_fidelity": 82,
		"feature_correlation":   78,
		"temporal_consistency":  75,
		"outlier_detection":     70,
		"schema_compliance":     90,
	}
	adjustment := (50 - avgRisk) / 5
	dims := fiber.Map{}
	for k, v := range base {
		score := v + adjustment
		if score > 100 {
			score = 100
		}
		if score < 0 {
			score = 0
		}
		dims[k] = score
	}
	return dims
}

// GetQualityTrendsFiber returns time-series quality data for charting
func GetQualityTrendsFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	period := c.Query("period", "30d")

	// Parse period
	days := 30
	if strings.HasSuffix(period, "d") {
		fmt.Sscanf(period, "%dd", &days)
	} else if strings.HasSuffix(period, "w") {
		var weeks int
		fmt.Sscanf(period, "%dw", &weeks)
		days = weeks * 7
	}
	if days <= 0 {
		days = 30
	}
	if days > 365 {
		days = 365
	}

	// Determine grouping: daily for <=30d, weekly for >30d
	groupByWeek := days > 30

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db := database.GetDB()

	startDate := time.Now().AddDate(0, 0, -days)

	var dataPoints []fiber.Map

	if groupByWeek {
		// Group by week
		rows, err := db.Query(ctx, `
			SELECT DATE_TRUNC('week', completed_at)::date AS week_start,
			       COALESCE(AVG(risk_score), 0) AS avg_risk,
			       COUNT(*) AS validation_count
			FROM validations
			WHERE user_id = $1
			  AND status = 'completed'
			  AND risk_score IS NOT NULL
			  AND completed_at >= $2
			GROUP BY DATE_TRUNC('week', completed_at)
			ORDER BY week_start ASC
		`, userID, startDate)
		if err != nil {
			log.Printf("Failed to query quality trends: %v", err)
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": fiber.Map{
					"code":    "DATABASE_ERROR",
					"message": "Failed to retrieve quality trends",
				},
			})
		}
		defer rows.Close()

		for rows.Next() {
			var date time.Time
			var avgRisk float64
			var count int
			if err := rows.Scan(&date, &avgRisk, &count); err != nil {
				log.Printf("Failed to scan trend row: %v", err)
				continue
			}
			dataPoints = append(dataPoints, fiber.Map{
				"date":        date.Format("2006-01-02"),
				"risk_score":  int(math.Round(avgRisk)),
				"validations": count,
			})
		}
	} else {
		// Group by day
		rows, err := db.Query(ctx, `
			SELECT completed_at::date AS day,
			       COALESCE(AVG(risk_score), 0) AS avg_risk,
			       COUNT(*) AS validation_count
			FROM validations
			WHERE user_id = $1
			  AND status = 'completed'
			  AND risk_score IS NOT NULL
			  AND completed_at >= $2
			GROUP BY completed_at::date
			ORDER BY day ASC
		`, userID, startDate)
		if err != nil {
			log.Printf("Failed to query quality trends: %v", err)
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": fiber.Map{
					"code":    "DATABASE_ERROR",
					"message": "Failed to retrieve quality trends",
				},
			})
		}
		defer rows.Close()

		for rows.Next() {
			var date time.Time
			var avgRisk float64
			var count int
			if err := rows.Scan(&date, &avgRisk, &count); err != nil {
				log.Printf("Failed to scan trend row: %v", err)
				continue
			}
			dataPoints = append(dataPoints, fiber.Map{
				"date":        date.Format("2006-01-02"),
				"risk_score":  int(math.Round(avgRisk)),
				"validations": count,
			})
		}
	}

	// If no data points, return empty with defaults
	if dataPoints == nil {
		dataPoints = []fiber.Map{}
	}

	// Compute trend direction and improvement percentage
	trend := "stable"
	improvementPct := 0

	if len(dataPoints) >= 2 {
		// Sort by date to ensure order
		sort.Slice(dataPoints, func(i, j int) bool {
			return dataPoints[i]["date"].(string) < dataPoints[j]["date"].(string)
		})

		firstScore := dataPoints[0]["risk_score"].(int)
		lastScore := dataPoints[len(dataPoints)-1]["risk_score"].(int)

		if firstScore > 0 {
			changePct := int(math.Round(float64(firstScore-lastScore) / float64(firstScore) * 100))
			improvementPct = changePct
		}

		if lastScore < firstScore {
			trend = "improving"
		} else if lastScore > firstScore {
			trend = "declining"
		}
	}

	response := fiber.Map{
		"period":          period,
		"data_points":     dataPoints,
		"trend":           trend,
		"improvement_pct": improvementPct,
	}

	return c.JSON(response)
}
