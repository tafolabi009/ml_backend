package handlers

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/tafolabi009/backend/go_backend/internal/repository"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
	"github.com/tafolabi009/backend/go_backend/pkg/pdfgen"
	"github.com/tafolabi009/backend/go_backend/pkg/storage"
)

// Report artifacts: when a validation completes we render the PDF report
// (and warranty certificate when eligible), upload them to the reports
// bucket, and store the object keys on the validation row. Reads then serve
// fresh, time-limited presigned URLs — the UI's download buttons expect
// report_url / certificate_url to simply work.

var reportsStorage *storage.S3Client

// SetReportsStorage wires the reports-bucket S3 client (set in main).
func SetReportsStorage(s3 *storage.S3Client) {
	reportsStorage = s3
}

// finishValidationSideEffects runs post-completion side effects shared by the
// real and simulated pipelines: owner notification + artifact generation.
// Best-effort; never fails the validation.
func finishValidationSideEffects(validationID string) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("finishValidationSideEffects(%s) panicked: %v", validationID, r)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := database.GetDB()

	var ownerID string
	var riskScore *int
	if err := db.QueryRow(ctx,
		`SELECT user_id, risk_score FROM validations WHERE id = $1`, validationID).
		Scan(&ownerID, &riskScore); err != nil {
		return
	}
	rs := 0
	if riskScore != nil {
		rs = *riskScore
	}
	insertNotification(ctx, ownerID, "validation_completed",
		"Validation completed",
		fmt.Sprintf("Validation %s finished with risk score %d. Report and certificate are ready.", validationID, rs))

	generateAndStoreArtifacts(ctx, validationID)
}

// generateAndStoreArtifacts renders and uploads the PDF artifacts, storing
// their object keys in report_url / certificate_url.
func generateAndStoreArtifacts(ctx context.Context, validationID string) {
	if reportsStorage == nil {
		return
	}
	db := database.GetDB()
	validationRepo := repository.NewValidationRepository(db)
	validation, err := validationRepo.GetByID(ctx, validationID)
	if err != nil || validation.Status != "completed" {
		return
	}
	results := sharedReportResults(db, validation)

	if pdf, perr := pdfgen.GenerateValidationReport(validation, results); perr == nil {
		key := "reports/" + validationID + ".pdf"
		if _, uerr := reportsStorage.Upload(ctx, key, bytes.NewReader(pdf),
			storage.UploadOptions{ContentType: "application/pdf"}); uerr == nil {
			_, _ = db.Exec(ctx, `UPDATE validations SET report_url = $2 WHERE id = $1`, validationID, key)
		} else {
			log.Printf("report upload failed for %s: %v", validationID, uerr)
		}
	} else {
		log.Printf("report generation failed for %s: %v", validationID, perr)
	}

	// certificate_url is the PUBLIC verify page for this certificate id;
	// third parties resolve it via GET /certificates/:id/verify. The PDF is
	// still uploaded under a deterministic key for authenticated download.
	verifyBase := os.Getenv("PUBLIC_APP_URL")
	if verifyBase == "" {
		verifyBase = "https://synthos.dev"
	}
	_, _ = db.Exec(ctx, `UPDATE validations SET certificate_url = $2 WHERE id = $1`,
		validationID, verifyBase+"/verify/cert_"+validationID)

	if validation.WarrantyEligible != nil && *validation.WarrantyEligible {
		if pdf, cerr := pdfgen.GenerateWarrantyCertificate(validation, "war_"+trimValPrefix(validationID)); cerr == nil {
			key := "certificates/" + validationID + ".pdf"
			if _, uerr := reportsStorage.Upload(ctx, key, bytes.NewReader(pdf),
				storage.UploadOptions{ContentType: "application/pdf"}); uerr != nil {
				log.Printf("certificate upload failed for %s: %v", validationID, uerr)
			}
		} else {
			log.Printf("certificate generation failed for %s: %v", validationID, cerr)
		}
	}
}

func trimValPrefix(validationID string) string {
	if len(validationID) > 4 && validationID[:4] == "val_" {
		return validationID[4:]
	}
	return validationID
}

// presignedArtifactURLs returns fresh time-limited URLs for stored artifact
// keys (empty strings when unavailable).
func presignedArtifactURLs(ctx context.Context, validationID string) (reportURL, certificateURL string) {
	if reportsStorage == nil {
		return "", ""
	}
	var reportKey, certKey string
	err := database.GetDB().QueryRow(ctx,
		`SELECT COALESCE(report_url, ''), COALESCE(certificate_url, '') FROM validations WHERE id = $1`,
		validationID).Scan(&reportKey, &certKey)
	if err != nil {
		return "", ""
	}
	const artifactURLTTL = 15 * time.Minute
	if reportKey != "" {
		if u, perr := reportsStorage.GeneratePresignedURL(ctx, reportKey, "GET", artifactURLTTL); perr == nil {
			reportURL = u
		}
	}
	// certificate_url is stored as the public verify-page URL; pass through.
	// Legacy rows that stored an S3 key get a presigned URL instead.
	if certKey != "" {
		if strings.HasPrefix(certKey, "http") {
			certificateURL = certKey
		} else if u, perr := reportsStorage.GeneratePresignedURL(ctx, certKey, "GET", artifactURLTTL); perr == nil {
			certificateURL = u
		}
	}
	return reportURL, certificateURL
}

// maybeNotifyCreditsLow warns the owner when the balance dips below 10% of
// lifetime purchases (or below 50 for accounts without purchases). Deduped:
// silent while an unread credits_low notification exists.
func maybeNotifyCreditsLow(ctx context.Context, userID string, balanceAfter int64) {
	var lifetime int64
	_ = database.GetDB().QueryRow(ctx,
		`SELECT COALESCE(lifetime_purchased, 0) FROM credit_balances WHERE user_id = $1`, userID).
		Scan(&lifetime)

	threshold := int64(50)
	if lifetime > 0 {
		threshold = lifetime / 10
	}
	if balanceAfter >= threshold {
		return
	}

	var unreadExists bool
	_ = database.GetDB().QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM notifications WHERE user_id = $1 AND type = 'credits_low' AND is_read = false)`,
		userID).Scan(&unreadExists)
	if unreadExists {
		return
	}

	insertNotification(ctx, userID, "credits_low",
		"Credits running low",
		fmt.Sprintf("Your balance is down to %d credits. Top up to keep validations and monitors running.", balanceAfter))
}
