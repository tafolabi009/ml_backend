package handlers

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/tafolabi009/backend/go_backend/internal/auth"
	"github.com/tafolabi009/backend/go_backend/internal/repository"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
)

// Contract wave for frontend PR #6. The UI detects missing features via 404,
// so these endpoints return 200 with empty/null bodies when there is simply
// no data.

// ---------------------------------------------------------------------------
// 1. Dataset groups
// ---------------------------------------------------------------------------

// getOrCreateDatasetGroup atomically creates or reuses the owner's group.
func getOrCreateDatasetGroup(ctx context.Context, ownerID, name string) (string, error) {
	db := database.GetDB()
	id := "grp_" + uuid.New().String()[:8]
	// Upsert keyed on (owner_id, name); archived groups are revived by reuse.
	var groupID string
	err := db.QueryRow(ctx,
		`INSERT INTO dataset_groups (id, owner_id, name)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (owner_id, name) DO UPDATE SET archived = false
		 RETURNING id`, id, ownerID, name).Scan(&groupID)
	return groupID, err
}

// ListDatasetGroupsFiber — GET /dataset-groups
func ListDatasetGroupsFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := database.GetDB().Query(ctx,
		`SELECT g.id, g.name, g.created_at,
		        COUNT(d.id) AS dataset_count,
		        COALESCE(SUM(d.file_size), 0) AS total_size
		 FROM dataset_groups g
		 LEFT JOIN datasets d ON d.group_id = g.id AND d.deleted_at IS NULL
		 WHERE g.owner_id = $1 AND g.archived = false
		 GROUP BY g.id, g.name, g.created_at
		 ORDER BY g.created_at DESC`, userID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "DATABASE_ERROR", "message": "Failed to list groups"},
		})
	}
	defer rows.Close()

	groups := []fiber.Map{}
	for rows.Next() {
		var id, name string
		var createdAt time.Time
		var count int
		var totalSize int64
		if err := rows.Scan(&id, &name, &createdAt, &count, &totalSize); err != nil {
			continue
		}
		statuses := fiber.Map{}
		srows, serr := database.GetDB().Query(ctx,
			`SELECT status, COUNT(*) FROM datasets WHERE group_id = $1 AND deleted_at IS NULL GROUP BY status`, id)
		if serr == nil {
			for srows.Next() {
				var st string
				var n int
				if srows.Scan(&st, &n) == nil {
					statuses[st] = n
				}
			}
			srows.Close()
		}
		groups = append(groups, fiber.Map{
			"id": id, "name": name, "dataset_count": count,
			"total_size_bytes": totalSize, "statuses": statuses, "created_at": createdAt,
		})
	}
	return c.JSON(fiber.Map{"groups": groups})
}

// DeleteDatasetGroupFiber — DELETE /dataset-groups/:id (archives group + datasets)
func DeleteDatasetGroupFiber(c *fiber.Ctx) error {
	groupID := c.Params("id")
	userID := c.Locals("user_id").(string)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := database.GetDB()

	tag, err := db.Exec(ctx,
		`UPDATE dataset_groups SET archived = true WHERE id = $1 AND owner_id = $2`, groupID, userID)
	if err != nil || tag.RowsAffected() == 0 {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{"code": "NOT_FOUND", "message": "Group not found"},
		})
	}
	_, _ = db.Exec(ctx,
		`UPDATE datasets SET deleted_at = NOW(), status = 'archived'
		 WHERE group_id = $1 AND user_id = $2 AND deleted_at IS NULL`, groupID, userID)
	return c.JSON(fiber.Map{"archived": true, "group_id": groupID})
}

// ---------------------------------------------------------------------------
// 3. Row-level findings — derived live from a sample of the dataset file.
// ---------------------------------------------------------------------------

// Ordered by severity/specificity: SSN and card patterns are strict subsets
// of the loose phone pattern, so they must be tried first.
var findingsPIIPatterns = []struct {
	kind string
	re   *regexp.Regexp
}{
	{"ssn", regexp.MustCompile(`^\d{3}-\d{2}-\d{4}$`)},
	{"credit_card", regexp.MustCompile(`^(?:\d[ -]?){13,19}$`)},
	{"email", regexp.MustCompile(`^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$`)},
	{"phone", regexp.MustCompile(`^\+?[0-9][0-9 ().-]{7,18}[0-9]$`)},
}

// GetValidationFindingsFiber — GET /validations/:id/findings
func GetValidationFindingsFiber(c *fiber.Ctx) error {
	validationID := c.Params("id")
	userID := c.Locals("user_id").(string)
	page, _ := strconv.Atoi(c.Query("page", "1"))
	if page < 1 {
		page = 1
	}
	pageSize, _ := strconv.Atoi(c.Query("page_size", "50"))
	if pageSize < 1 || pageSize > 200 {
		pageSize = 50
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	db := database.GetDB()

	validation, err := repository.NewValidationRepository(db).GetByID(ctx, validationID)
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

	findings := []fiber.Map{}
	dataset, derr := repository.NewDatasetRepository(db).GetByID(ctx, validation.DatasetID)
	if derr == nil {
		if parsed, perr := loadDatasetSample(ctx, dataset); perr == nil {
			findings = deriveRowFindings(parsed)
		}
	}
	// Clean (or unpreviewable) validations: empty list with total 0 — 200, not 404.

	total := len(findings)
	start := (page - 1) * pageSize
	if start > total {
		start = total
	}
	end := start + pageSize
	if end > total {
		end = total
	}
	totalPages := (total + pageSize - 1) / pageSize
	return c.JSON(fiber.Map{
		"findings": findings[start:end],
		"total":    total,
		"pagination": fiber.Map{
			"page": page, "per_page": pageSize, "total": total, "total_pages": totalPages,
		},
	})
}

// deriveRowFindings scans sampled rows for PII values, nulls in mostly-filled
// columns, and extreme numeric outliers.
func deriveRowFindings(p *parsedDataset) []fiber.Map {
	findings := []fiber.Map{}
	if len(p.rows) == 0 {
		return findings
	}
	nCols := len(p.columns)

	// Column profiles: null rate + numeric mean/std.
	nullCounts := make([]int, nCols)
	sums := make([]float64, nCols)
	sqSums := make([]float64, nCols)
	numCounts := make([]int, nCols)
	for _, row := range p.rows {
		for i := 0; i < nCols; i++ {
			v := row[i]
			if nullTokens[v] {
				nullCounts[i]++
				continue
			}
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				sums[i] += f
				sqSums[i] += f * f
				numCounts[i]++
			}
		}
	}

	rowsN := float64(len(p.rows))
	const maxFindings = 500
	for rowIdx, row := range p.rows {
		if len(findings) >= maxFindings {
			break
		}
		for i := 0; i < nCols; i++ {
			v := row[i]
			col := p.columns[i]

			// Missing value in a column that is >95% filled.
			if nullTokens[v] {
				if float64(nullCounts[i])/rowsN < 0.05 {
					findings = append(findings, fiber.Map{
						"row_index": rowIdx, "column": col, "severity": "medium",
						"issue":  "missing_value",
						"detail": "Empty value in a column that is otherwise >95% populated",
					})
				}
				continue
			}
			// PII in cell values (deterministic severity order).
			for _, pat := range findingsPIIPatterns {
				if pat.re.MatchString(v) {
					sev := "high"
					if pat.kind == "ssn" || pat.kind == "credit_card" {
						if pat.kind == "credit_card" && !luhnOK(strings.NewReplacer(" ", "", "-", "").Replace(v)) {
							continue
						}
						sev = "critical"
					}
					findings = append(findings, fiber.Map{
						"row_index": rowIdx, "column": col, "severity": sev,
						"issue":  "pii_" + pat.kind,
						"detail": "Value matches a " + pat.kind + " pattern",
						"sample": truncateValue(v, 40),
					})
					break
				}
			}
			// Extreme numeric outlier (>4 sigma).
			if numCounts[i] >= 30 {
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					mean := sums[i] / float64(numCounts[i])
					variance := sqSums[i]/float64(numCounts[i]) - mean*mean
					if variance > 0 {
						std := math.Sqrt(variance)
						if std > 0 && (f > mean+4*std || f < mean-4*std) {
							findings = append(findings, fiber.Map{
								"row_index": rowIdx, "column": col, "severity": "low",
								"issue":  "numeric_outlier",
								"detail": fmt.Sprintf("Value deviates more than 4 standard deviations from the column mean (%.2f)", mean),
								"sample": truncateValue(v, 40),
							})
						}
					}
				}
			}
		}
	}
	return findings
}

func luhnOK(digits string) bool {
	total, alt := 0, false
	for i := len(digits) - 1; i >= 0; i-- {
		ch := digits[i]
		if ch < '0' || ch > '9' {
			return false
		}
		d := int(ch - '0')
		if alt {
			d *= 2
			if d > 9 {
				d -= 9
			}
		}
		total += d
		alt = !alt
	}
	return total%10 == 0
}

// ---------------------------------------------------------------------------
// 7. Admin growth
// ---------------------------------------------------------------------------

// GetAdminGrowthFiber — GET /admin/analytics/growth?period=30d
func GetAdminGrowthFiber(c *fiber.Ctx) error {
	days := 30
	if p := c.Query("period", "30d"); strings.HasSuffix(p, "d") {
		if n, err := strconv.Atoi(strings.TrimSuffix(p, "d")); err == nil && n >= 1 && n <= 365 {
			days = n
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db := database.GetDB()
	start := time.Now().AddDate(0, 0, -days)

	signups := map[string]int{}
	if rows, err := db.Query(ctx,
		`SELECT created_at::date, COUNT(*) FROM users WHERE created_at >= $1 GROUP BY 1`, start); err == nil {
		for rows.Next() {
			var d time.Time
			var n int
			if rows.Scan(&d, &n) == nil {
				signups[d.Format("2006-01-02")] = n
			}
		}
		rows.Close()
	}
	validations := map[string]int{}
	if rows, err := db.Query(ctx,
		`SELECT created_at::date, COUNT(*) FROM validations WHERE created_at >= $1 GROUP BY 1`, start); err == nil {
		for rows.Next() {
			var d time.Time
			var n int
			if rows.Scan(&d, &n) == nil {
				validations[d.Format("2006-01-02")] = n
			}
		}
		rows.Close()
	}

	points := []fiber.Map{}
	for d := 0; d <= days; d++ {
		date := start.AddDate(0, 0, d).Format("2006-01-02")
		points = append(points, fiber.Map{
			"date": date, "signups": signups[date], "validations": validations[date],
		})
	}
	return c.JSON(fiber.Map{"period": fmt.Sprintf("%dd", days), "points": points})
}

// ---------------------------------------------------------------------------
// 8. Two-factor auth contract aliases
// ---------------------------------------------------------------------------

// TwoFactorStatusFiber — GET /auth/2fa/status
func TwoFactorStatusFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var enabled bool
	_ = database.GetDB().QueryRow(ctx,
		`SELECT COALESCE(two_factor_enabled, false) FROM users WHERE id = $1`, userID).Scan(&enabled)
	return c.JSON(fiber.Map{"enabled": enabled})
}

// TwoFactorEnrollFiber — POST /auth/2fa/enroll -> {secret, otpauth_url}
func TwoFactorEnrollFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := database.GetDB()
	userRepo := repository.NewUserRepository(db)

	user, err := userRepo.GetByID(ctx, userID)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{"code": "NOT_FOUND", "message": "User not found"},
		})
	}
	if user.TwoFactorEnabled {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "ALREADY_ENABLED", "message": "Two-factor auth is already enabled"},
		})
	}

	secret, otpauthURL, err := auth.GenerateTOTPSecret(user.Email)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "TOTP_ERROR", "message": "Failed to generate 2FA secret"},
		})
	}
	// Pending until activate; recovery codes are generated fresh at activation.
	if err := userRepo.StorePending2FASecret(ctx, userID, secret, nil); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "DATABASE_ERROR", "message": "Failed to store pending secret"},
		})
	}
	return c.JSON(fiber.Map{"secret": secret, "otpauth_url": otpauthURL})
}

// TwoFactorActivateFiber — POST /auth/2fa/activate {code} -> {recovery_codes}
func TwoFactorActivateFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	var req struct {
		Code string `json:"code"`
	}
	if err := c.BodyParser(&req); err != nil || strings.TrimSpace(req.Code) == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_REQUEST", "message": "code is required"},
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	userRepo := repository.NewUserRepositoryWithPool(database.GetDB())

	pendingSecret, _, err := userRepo.GetPending2FASecret(ctx, userID)
	if err != nil || pendingSecret == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "NO_PENDING_ENROLLMENT", "message": "Start enrollment first via /auth/2fa/enroll"},
		})
	}
	if !auth.ValidateTOTPCode(strings.TrimSpace(req.Code), pendingSecret) {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_CODE", "message": "Invalid verification code"},
		})
	}

	recoveryCodes, err := auth.GenerateBackupCodes(10)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "TOTP_ERROR", "message": "Failed to generate recovery codes"},
		})
	}
	if err := userRepo.Enable2FA(ctx, userID, pendingSecret, auth.HashBackupCodes(recoveryCodes)); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "DATABASE_ERROR", "message": "Failed to enable 2FA"},
		})
	}
	insertNotification(ctx, userID, "security", "Two-factor authentication enabled",
		"TOTP two-factor auth is now required at login for your account.")
	return c.JSON(fiber.Map{"recovery_codes": recoveryCodes})
}

// ---------------------------------------------------------------------------
// 9. Public certificate verification by id
// ---------------------------------------------------------------------------

// VerifyCertificateByIDFiber — GET /certificates/:id/verify (PUBLIC)
func VerifyCertificateByIDFiber(c *fiber.Ctx) error {
	certID := c.Params("id")
	invalid := fiber.Map{"valid": false, "certificate_id": certID}
	if !strings.HasPrefix(certID, "cert_") {
		return c.JSON(invalid)
	}
	validationID := strings.TrimPrefix(certID, "cert_")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := database.GetDB()

	var status, datasetName, issuedTo string
	var riskScore *int
	var completedAt *time.Time
	err := db.QueryRow(ctx,
		`SELECT v.status, COALESCE(d.filename, ''), v.risk_score, v.completed_at,
		        COALESCE(u.company_name, COALESCE(u.full_name, ''))
		 FROM validations v
		 LEFT JOIN datasets d ON d.id = v.dataset_id
		 LEFT JOIN users u ON u.id = v.user_id
		 WHERE v.id = $1`, validationID).
		Scan(&status, &datasetName, &riskScore, &completedAt, &issuedTo)
	if err != nil || status != "completed" || completedAt == nil {
		return c.JSON(invalid)
	}

	expiresAt := completedAt.Add(90 * 24 * time.Hour)
	resp := fiber.Map{
		"valid":          time.Now().Before(expiresAt),
		"certificate_id": certID,
		"validation_id":  validationID,
		"dataset_name":   datasetName,
		"risk_score":     riskScore,
		"issued_at":      completedAt.UTC().Format(time.RFC3339),
		"expires_at":     expiresAt.UTC().Format(time.RFC3339),
	}
	if issuedTo != "" {
		resp["issued_to"] = issuedTo
	}
	return c.JSON(resp)
}

// ---------------------------------------------------------------------------
// 11. Notification delete
// ---------------------------------------------------------------------------

// DeleteNotificationFiber — DELETE /notifications/:id
func DeleteNotificationFiber(c *fiber.Ctx) error {
	notifID := c.Params("id")
	userID := c.Locals("user_id").(string)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tag, err := database.GetDB().Exec(ctx,
		`DELETE FROM notifications WHERE id = $1 AND user_id = $2`, notifID, userID)
	if err != nil || tag.RowsAffected() == 0 {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{"code": "NOT_FOUND", "message": "Notification not found"},
		})
	}
	return c.JSON(fiber.Map{"deleted": true, "id": notifID})
}
