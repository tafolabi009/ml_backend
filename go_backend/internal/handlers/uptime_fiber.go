package handlers

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
)

// Real uptime for the public status page, computed from health samples the
// background scheduler records every tick. No hardcoded claims: until 30
// days of history accrue the response says exactly since when it measures.

// recordHealthChecks samples service health; called from the scheduler tick.
func recordHealthChecks(ctx context.Context) {
	db := database.GetDB()
	insert := func(service string, healthy bool, latency time.Duration) {
		_, err := db.Exec(ctx,
			`INSERT INTO service_health_checks (service, healthy, latency_ms) VALUES ($1, $2, $3)`,
			service, healthy, int(latency.Milliseconds()))
		if err != nil {
			return // table missing or db down; nothing sensible to do
		}
	}

	// Database (implicitly healthy if this insert path works, but measure a ping).
	start := time.Now()
	dbHealthy := db.Ping(ctx) == nil
	insert("database", dbHealthy, time.Since(start))

	// API gateway: this process is alive and serving if the scheduler runs.
	insert("api", true, 0)

	// Validation ML service via its gRPC health probe.
	if validationGRPCClient != nil {
		hctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		start = time.Now()
		err := validationGRPCClient.Health(hctx, "uptime-probe")
		cancel()
		insert("validation_engine", err == nil, time.Since(start))
	}

	// Retention: keep ~35 days.
	_, _ = db.Exec(ctx, `DELETE FROM service_health_checks WHERE checked_at < NOW() - INTERVAL '35 days'`)
}

// GetUptimeFiber serves 30d uptime + incident list. PUBLIC.
// GET /health/uptime
func GetUptimeFiber(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := database.GetDB()

	rows, err := db.Query(ctx,
		`SELECT service,
		        COUNT(*) AS total,
		        COUNT(*) FILTER (WHERE healthy) AS ok,
		        MIN(checked_at) AS since
		 FROM service_health_checks
		 WHERE checked_at > NOW() - INTERVAL '30 days'
		 GROUP BY service ORDER BY service`)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{"code": "DATABASE_ERROR", "message": "Failed to compute uptime"},
		})
	}
	defer rows.Close()

	type svcRow struct {
		service   string
		total, ok int64
		since     time.Time
	}
	var svcs []svcRow
	for rows.Next() {
		var r svcRow
		if err := rows.Scan(&r.service, &r.total, &r.ok, &r.since); err != nil {
			continue
		}
		svcs = append(svcs, r)
	}

	checks := []fiber.Map{}
	var weightedSum float64
	var totalChecks int64
	var earliest *time.Time
	for _, s := range svcs {
		pct := 100.0
		if s.total > 0 {
			pct = math.Round(float64(s.ok)/float64(s.total)*10000) / 100
		}
		checks = append(checks, fiber.Map{
			"service":        s.service,
			"uptime_30d_pct": pct,
			"samples":        s.total,
		})
		weightedSum += float64(s.ok)
		totalChecks += s.total
		if earliest == nil || s.since.Before(*earliest) {
			t := s.since
			earliest = &t
		}
	}

	overall := 100.0
	if totalChecks > 0 {
		overall = math.Round(weightedSum/float64(totalChecks)*10000) / 100
	}

	// Incidents: contiguous unhealthy runs of >= 3 samples per service.
	incidents := []fiber.Map{}
	irows, ierr := db.Query(ctx,
		`SELECT service, healthy, checked_at FROM service_health_checks
		 WHERE checked_at > NOW() - INTERVAL '30 days'
		 ORDER BY service, checked_at`)
	if ierr == nil {
		defer irows.Close()
		var curService string
		var runStart time.Time
		var runLen int
		var lastAt time.Time
		flush := func(resolved bool) {
			if runLen >= 3 {
				mins := int(lastAt.Sub(runStart).Minutes()) + 1
				incidents = append(incidents, fiber.Map{
					"date":     runStart.UTC().Format(time.RFC3339),
					"summary":  fmt.Sprintf("%s degraded for ~%d min", curService, mins),
					"resolved": resolved,
				})
			}
			runLen = 0
		}
		prevService := ""
		for irows.Next() {
			var service string
			var healthy bool
			var at time.Time
			if err := irows.Scan(&service, &healthy, &at); err != nil {
				continue
			}
			if service != prevService {
				flush(true)
				prevService = service
			}
			if !healthy {
				if runLen == 0 {
					curService, runStart = service, at
				}
				runLen++
				lastAt = at
			} else {
				flush(true)
			}
		}
		flush(false) // an unhealthy tail is an ongoing incident
	}
	// Newest first, cap 10.
	for i, j := 0, len(incidents)-1; i < j; i, j = i+1, j-1 {
		incidents[i], incidents[j] = incidents[j], incidents[i]
	}
	if len(incidents) > 10 {
		incidents = incidents[:10]
	}

	resp := fiber.Map{
		"uptime_30d_pct": overall,
		"checks":         checks,
		"incidents":      incidents,
	}
	if earliest != nil {
		resp["measuring_since"] = earliest.UTC().Format(time.RFC3339)
		if time.Since(*earliest) < 30*24*time.Hour {
			resp["note"] = "Uptime measured from health samples collected since measuring_since (less than a full 30-day window so far)."
		}
	} else {
		resp["note"] = "No health samples recorded yet."
	}
	return c.JSON(resp)
}
