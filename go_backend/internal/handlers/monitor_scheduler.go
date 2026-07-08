package handlers

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/tafolabi009/backend/go_backend/internal/models"
	"github.com/tafolabi009/backend/go_backend/internal/repository"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
	"github.com/tafolabi009/backend/go_backend/pkg/webhook"
)

// Background scheduler: drives drift monitors, evaluates finished runs,
// reconciles refunds for failed validations, and purges expired idempotency
// keys. All steps are written to be safe when multiple gateway instances run
// the loop concurrently (conditional-UPDATE claims, idempotent refunds).

// StartMonitorScheduler launches the background loop. Call once from main.
func StartMonitorScheduler(ctx context.Context) {
	if os.Getenv("MONITOR_SCHEDULER") == "off" {
		log.Println("Monitor scheduler disabled via MONITOR_SCHEDULER=off")
		return
	}
	interval := 60 * time.Second
	if v := os.Getenv("MONITOR_SCHEDULER_INTERVAL_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 10 {
			interval = time.Duration(n) * time.Second
		}
	}

	go func() {
		log.Printf("✅ Monitor scheduler started (interval %s)", interval)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				log.Println("Monitor scheduler stopped")
				return
			case <-ticker.C:
				runSchedulerTick()
			}
		}
	}()
}

func runSchedulerTick() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("scheduler tick panicked: %v", r)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	evaluateFinishedMonitorRuns(ctx)
	triggerDueMonitors(ctx)
	reconcileFailedValidationRefunds(ctx)
	purgeExpiredIdempotencyKeys(ctx)
	recordHealthChecks(ctx)
}

// evaluateFinishedMonitorRuns closes out pending runs whose validation has
// finished, updates monitor state, and alerts on threshold breaches.
func evaluateFinishedMonitorRuns(ctx context.Context) {
	db := database.GetDB()
	rows, err := db.Query(ctx,
		`SELECT mr.id, mr.monitor_id, mr.validation_id, v.status, v.risk_score,
		        m.user_id, m.max_risk_score, m.name, m.dataset_id
		 FROM monitor_runs mr
		 JOIN validations v ON v.id = mr.validation_id
		 JOIN dataset_monitors m ON m.id = mr.monitor_id
		 WHERE mr.status = 'pending' AND v.status IN ('completed', 'failed', 'cancelled')
		 LIMIT 50`)
	if err != nil {
		log.Printf("scheduler: evaluate query failed: %v", err)
		return
	}
	type finishedRun struct {
		runID, monitorID, validationID, vStatus string
		riskScore                               *int
		userID, monitorName, datasetID          string
		maxRisk                                 int
	}
	var runs []finishedRun
	for rows.Next() {
		var r finishedRun
		if err := rows.Scan(&r.runID, &r.monitorID, &r.validationID, &r.vStatus, &r.riskScore,
			&r.userID, &r.maxRisk, &r.monitorName, &r.datasetID); err != nil {
			continue
		}
		runs = append(runs, r)
	}
	rows.Close()

	for _, r := range runs {
		alerted := false
		risk := -1
		if r.vStatus == "completed" && r.riskScore != nil {
			risk = *r.riskScore
			alerted = risk > r.maxRisk
		}

		// Claim the run so only one instance evaluates/alerts it.
		tag, err := db.Exec(ctx,
			`UPDATE monitor_runs SET status = $2, risk_score = $3, alerted = $4, evaluated_at = NOW()
			 WHERE id = $1 AND status = 'pending'`,
			r.runID, r.vStatus, nullableInt(risk), alerted)
		if err != nil || tag.RowsAffected() == 0 {
			continue // another instance won the claim
		}

		if r.vStatus == "completed" {
			_, _ = db.Exec(ctx,
				`UPDATE dataset_monitors
				 SET last_risk_score = $2,
				     consecutive_alerts = CASE WHEN $3 THEN consecutive_alerts + 1 ELSE 0 END,
				     updated_at = NOW()
				 WHERE id = $1`,
				r.monitorID, nullableInt(risk), alerted)
		}

		if r.vStatus == "completed" && !alerted {
			insertNotification(ctx, r.userID, "scheduled_run",
				fmt.Sprintf("Scheduled validation passed: %s", r.monitorName),
				fmt.Sprintf("Validation %s completed with risk score %d (threshold %d).", r.validationID, risk, r.maxRisk))
		}

		if alerted {
			payload := fiber.Map{
				"monitor_id":     r.monitorID,
				"monitor_name":   r.monitorName,
				"dataset_id":     r.datasetID,
				"validation_id":  r.validationID,
				"risk_score":     risk,
				"max_risk_score": r.maxRisk,
			}
			webhook.Dispatch("monitor.alert", r.userID, payload)
			insertNotification(ctx, r.userID, "monitor_alert",
				fmt.Sprintf("Drift alert: %s", r.monitorName),
				fmt.Sprintf("Scheduled validation %s scored risk %d (threshold %d). Your dataset quality may be drifting.",
					r.validationID, risk, r.maxRisk))
			log.Printf("🔔 monitor alert: %s risk=%d threshold=%d", r.monitorID, risk, r.maxRisk)
		}
	}
}

// triggerDueMonitors starts validations for monitors whose next_run_at has
// passed. The conditional UPDATE claim makes this multi-instance safe.
func triggerDueMonitors(ctx context.Context) {
	db := database.GetDB()
	rows, err := db.Query(ctx,
		`SELECT m.id, m.user_id, m.dataset_id, m.name, m.validation_type, m.interval_hours
		 FROM dataset_monitors m
		 JOIN datasets d ON d.id = m.dataset_id
		 WHERE m.is_active = true AND m.next_run_at <= NOW()
		   AND d.status IN ('processed', 'ready')
		   AND NOT EXISTS (SELECT 1 FROM monitor_runs mr WHERE mr.monitor_id = m.id AND mr.status = 'pending')
		 LIMIT 10`)
	if err != nil {
		log.Printf("scheduler: due-monitor query failed: %v", err)
		return
	}
	type dueMonitor struct {
		id, userID, datasetID, name, vtype string
		intervalHours                      int
	}
	var due []dueMonitor
	for rows.Next() {
		var m dueMonitor
		if err := rows.Scan(&m.id, &m.userID, &m.datasetID, &m.name, &m.vtype, &m.intervalHours); err != nil {
			continue
		}
		due = append(due, m)
	}
	rows.Close()

	creditRepo := repository.NewCreditRepository(db)
	for _, m := range due {
		// Claim: only the instance that flips next_run_at runs this cycle.
		tag, err := db.Exec(ctx,
			`UPDATE dataset_monitors
			 SET last_run_at = NOW(), next_run_at = NOW() + ($2 * INTERVAL '1 hour'), updated_at = NOW()
			 WHERE id = $1 AND next_run_at <= NOW()`,
			m.id, m.intervalHours)
		if err != nil || tag.RowsAffected() == 0 {
			continue
		}

		cost := int64(25)
		if cc, cerr := creditRepo.GetCreditCostByOperation(ctx, "validation_standard"); cerr == nil {
			cost = cc.CreditsRequired
		}

		validationID := "val_" + uuid.New().String()[:8]
		validation := models.Validation{
			ID:                  validationID,
			DatasetID:           m.datasetID,
			UserID:              m.userID,
			Status:              "queued",
			EstimatedCompletion: time.Now().Add(24 * time.Hour),
		}
		txn, err := creditRepo.CreateValidationCharged(ctx, &validation, m.vtype, "standard", cost,
			fmt.Sprintf("Scheduled monitor run: %s (%s)", m.name, m.id))
		if err == nil {
			maybeNotifyCreditsLow(ctx, m.userID, txn.BalanceAfter)
		}
		if err != nil {
			if errors.Is(err, repository.ErrInsufficientCredits) {
				_, _ = db.Exec(ctx,
					`UPDATE dataset_monitors SET is_active = false, paused_reason = 'insufficient_credits', updated_at = NOW() WHERE id = $1`,
					m.id)
				webhook.Dispatch("monitor.paused", m.userID, fiber.Map{
					"monitor_id": m.id, "monitor_name": m.name, "reason": "insufficient_credits",
				})
				insertNotification(ctx, m.userID, "monitor_paused",
					fmt.Sprintf("Monitor paused: %s", m.name),
					"Scheduled validation could not run because your credit balance is too low. Top up and resume the monitor.")
				log.Printf("monitor %s paused: insufficient credits", m.id)
			} else {
				log.Printf("monitor %s run failed to create validation: %v", m.id, err)
			}
			continue
		}

		_, err = db.Exec(ctx,
			`INSERT INTO monitor_runs (id, monitor_id, validation_id, status) VALUES ($1, $2, $3, 'pending')`,
			"mrun_"+uuid.New().String()[:12], m.id, validationID)
		if err != nil {
			log.Printf("monitor %s: failed to record run: %v", m.id, err)
		}
		_, _ = db.Exec(ctx,
			`UPDATE dataset_monitors SET last_validation_id = $2, updated_at = NOW() WHERE id = $1`,
			m.id, validationID)

		webhook.Dispatch("monitor.run_started", m.userID, fiber.Map{
			"monitor_id": m.id, "validation_id": validationID, "dataset_id": m.datasetID,
		})
		go processValidationAsync(validationID, m.datasetID, m.userID, m.vtype)
		log.Printf("📡 monitor %s started validation %s", m.id, validationID)
	}
}

// reconcileFailedValidationRefunds refunds charges for validations that ended
// in 'failed' without a refund. RefundValidationCharge is idempotent, so this
// is safe to run from every instance on every tick.
func reconcileFailedValidationRefunds(ctx context.Context) {
	db := database.GetDB()
	rows, err := db.Query(ctx,
		`SELECT v.id, v.user_id FROM validations v
		 WHERE v.status = 'failed'
		   AND EXISTS (SELECT 1 FROM credit_transactions ct WHERE ct.reference_id = v.id AND ct.type = 'deduction')
		   AND NOT EXISTS (SELECT 1 FROM credit_transactions ct WHERE ct.reference_id = v.id AND ct.type = 'refund')
		 LIMIT 25`)
	if err != nil {
		log.Printf("scheduler: refund reconcile query failed: %v", err)
		return
	}
	type failedVal struct{ id, userID string }
	var failed []failedVal
	for rows.Next() {
		var f failedVal
		if err := rows.Scan(&f.id, &f.userID); err != nil {
			continue
		}
		failed = append(failed, f)
	}
	rows.Close()

	creditRepo := repository.NewCreditRepository(db)
	for _, f := range failed {
		amount, refunded, err := creditRepo.RefundValidationCharge(ctx, f.id, f.userID,
			fmt.Sprintf("Automatic refund: validation %s failed", f.id))
		if err != nil {
			log.Printf("auto-refund for %s failed: %v", f.id, err)
			continue
		}
		if refunded {
			webhook.Dispatch("validation.refunded", f.userID, fiber.Map{
				"validation_id": f.id, "refund_amount": amount, "reason": "validation_failed",
			})
			insertNotification(ctx, f.userID, "validation_failed",
				"Validation failed",
				fmt.Sprintf("Validation %s failed; %d credits were automatically refunded.", f.id, amount))
			log.Printf("💸 auto-refunded %d credits for failed validation %s", amount, f.id)
		}
	}
}

// purgeExpiredIdempotencyKeys removes idempotency rows past their TTL.
func purgeExpiredIdempotencyKeys(ctx context.Context) {
	_, err := database.GetDB().Exec(ctx, `DELETE FROM idempotency_keys WHERE expires_at < NOW()`)
	if err != nil {
		log.Printf("scheduler: idempotency purge failed: %v", err)
	}
}

// insertNotification best-effort inserts an in-app notification.
func insertNotification(ctx context.Context, userID, ntype, title, message string) {
	_, err := database.GetDB().Exec(ctx,
		`INSERT INTO notifications (id, user_id, type, title, message) VALUES ($1, $2, $3, $4, $5)`,
		"ntf_"+uuid.New().String()[:12], userID, ntype, title, message)
	if err != nil {
		log.Printf("notification insert failed: %v", err)
	}
}

// nullableInt maps -1 (unknown) to NULL for INSERT/UPDATE parameters.
func nullableInt(v int) interface{} {
	if v < 0 {
		return nil
	}
	return v
}
