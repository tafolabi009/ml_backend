package middleware

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/tafolabi009/backend/go_backend/pkg/database"
)

// ImpersonationWriteBlocked reports whether a request made under an
// impersonation token must be rejected. Sessions are read-mostly: only safe
// verbs pass, so destructive and billing actions are impossible while
// impersonating. Logout is allowed so the admin can end the session cleanly.
func ImpersonationWriteBlocked(method, path string) bool {
	switch method {
	case "GET", "HEAD", "OPTIONS":
		return false
	}
	if path == "/api/v1/auth/logout" {
		return false
	}
	return true
}

// auditImpersonatedRequest records every impersonated request with both
// identities (best effort, async — never blocks the request path).
func auditImpersonatedRequest(impersonatorID, targetUserID, method, path, ip string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		db := database.GetDB()
		if db == nil {
			return
		}
		details, _ := json.Marshal(map[string]string{
			"impersonator_id": impersonatorID,
			"method":          method,
			"path":            path,
		})
		if _, err := db.Exec(ctx,
			`INSERT INTO security_events (user_id, event_type, success, ip_address, details)
			 VALUES ($1, 'impersonated_request', true, $2, $3)`,
			targetUserID, ip, details); err != nil {
			log.Printf("impersonation audit insert failed: %v", err)
		}
	}()
}
