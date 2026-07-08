package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
)

// Idempotency support for charging endpoints (validation create, credit
// purchase). Clients send an Idempotency-Key header; a retried request with
// the same key replays the original successful response instead of charging
// again. Semantics follow the common payment-API pattern:
//
//   - first request with a key claims it (a pending row) and executes;
//   - a concurrent duplicate gets 409 IDEMPOTENCY_IN_PROGRESS;
//   - a retry after success replays the stored status+body verbatim;
//   - the same key with a DIFFERENT body is rejected with 422;
//   - failed executions release the key so the client can retry.
//
// Keys expire after 24h; expired rows are purged by the background scheduler.

const idemTTL = 24 * time.Hour

// idemGuard tracks ownership of an idempotency key for the current request.
type idemGuard struct {
	active   bool // header present and row claimed by this request
	stored   bool // successful response persisted
	userID   string
	endpoint string
	key      string
}

// beginIdempotency claims the request's Idempotency-Key (if any).
// When handled is true the response has already been written (replay,
// in-progress conflict, or body mismatch) and the handler must return err.
func beginIdempotency(c *fiber.Ctx, ctx context.Context, endpoint string) (*idemGuard, bool, error) {
	g := &idemGuard{}
	key := strings.TrimSpace(c.Get("Idempotency-Key"))
	if key == "" {
		return g, false, nil // no key: idempotency not requested
	}
	if len(key) > 255 {
		return g, true, c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_IDEMPOTENCY_KEY", "message": "Idempotency-Key must be at most 255 characters"},
		})
	}

	userID, _ := c.Locals("user_id").(string)
	sum := sha256.Sum256(c.Body())
	reqHash := hex.EncodeToString(sum[:])
	db := database.GetDB()

	// Try to claim the key (response_status = 0 marks "in progress").
	tag, err := db.Exec(ctx,
		`INSERT INTO idempotency_keys (user_id, endpoint, idem_key, request_hash, response_status, expires_at)
		 VALUES ($1, $2, $3, $4, 0, NOW() + $5::interval)
		 ON CONFLICT (user_id, endpoint, idem_key) DO NOTHING`,
		userID, endpoint, key, reqHash, idemTTL.String())
	if err != nil {
		// Fail open: idempotency is a safety net, not a gate. Log and proceed.
		log.Printf("idempotency claim failed (proceeding without): %v", err)
		return g, false, nil
	}

	if tag.RowsAffected() == 1 {
		g.active, g.userID, g.endpoint, g.key = true, userID, endpoint, key
		return g, false, nil
	}

	// Key already exists: replay, in-progress, or mismatch.
	var storedHash string
	var status int
	var body []byte
	err = db.QueryRow(ctx,
		`SELECT request_hash, response_status, COALESCE(response_body, 'null'::jsonb)
		 FROM idempotency_keys
		 WHERE user_id = $1 AND endpoint = $2 AND idem_key = $3 AND expires_at > NOW()`,
		userID, endpoint, key).Scan(&storedHash, &status, &body)
	if err != nil {
		// Row expired between INSERT and SELECT (or read failed): proceed fresh.
		log.Printf("idempotency lookup failed (proceeding without): %v", err)
		return g, false, nil
	}

	if storedHash != reqHash {
		return g, true, c.Status(fiber.StatusUnprocessableEntity).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "IDEMPOTENCY_KEY_REUSED",
				"message": "This Idempotency-Key was already used with a different request body",
			},
		})
	}
	if status == 0 {
		return g, true, c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "IDEMPOTENCY_IN_PROGRESS",
				"message": "A request with this Idempotency-Key is still being processed; retry shortly",
			},
		})
	}

	c.Set("Idempotent-Replay", "true")
	c.Type("json")
	return g, true, c.Status(status).Send(body)
}

// finish persists the successful response so future retries replay it.
func (g *idemGuard) finish(ctx context.Context, status int, jsonBody []byte) {
	if !g.active {
		return
	}
	_, err := database.GetDB().Exec(ctx,
		`UPDATE idempotency_keys SET response_status = $4, response_body = $5
		 WHERE user_id = $1 AND endpoint = $2 AND idem_key = $3`,
		g.userID, g.endpoint, g.key, status, jsonBody)
	if err != nil {
		log.Printf("idempotency store failed for %s/%s: %v", g.endpoint, g.key, err)
		return
	}
	g.stored = true
}

// release frees a claimed key after a failed execution so the client can
// retry with the same key. Call via defer; it is a no-op after finish().
func (g *idemGuard) release() {
	if !g.active || g.stored {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = database.GetDB().Exec(ctx,
		`DELETE FROM idempotency_keys
		 WHERE user_id = $1 AND endpoint = $2 AND idem_key = $3 AND response_status = 0`,
		g.userID, g.endpoint, g.key)
}
