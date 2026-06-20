package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lib/pq"
	"github.com/tafolabi009/backend/go_backend/internal/models"
)

// Extended UserRepository methods for authentication features

// UpdateLoginAttempts updates the failed login attempts counter
func (r *UserRepository) UpdateLoginAttempts(ctx context.Context, userID string, attempts int, lockedUntil *time.Time) error {
	query := `
		UPDATE users
		SET failed_login_attempts = $2, locked_until = $3, updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`
	_, err := r.db.Exec(ctx, query, userID, attempts, lockedUntil)
	return err
}

// UpdateLastLogin updates the last login timestamp
func (r *UserRepository) UpdateLastLogin(ctx context.Context, userID string, lastLogin time.Time) error {
	query := `
		UPDATE users
		SET last_login_at = $2, updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`
	_, err := r.db.Exec(ctx, query, userID, lastLogin)
	return err
}

// UpdatePassword updates user's password hash
func (r *UserRepository) UpdatePassword(ctx context.Context, userID string, passwordHash string, changedAt time.Time) error {
	query := `
		UPDATE users
		SET password_hash = $2, password_changed_at = $3, updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`
	_, err := r.db.Exec(ctx, query, userID, passwordHash, changedAt)
	return err
}

// UpdateBackupCodes updates user's 2FA backup codes
func (r *UserRepository) UpdateBackupCodes(ctx context.Context, userID string, codes []string) error {
	query := `
		UPDATE users
		SET two_factor_backup_codes = $2, updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`
	_, err := r.db.Exec(ctx, query, userID, pq.Array(codes))
	return err
}

// Session Management

// CreateSession creates a new session record
func (r *UserRepository) CreateSession(ctx context.Context, session *models.Session) error {
	query := `
		INSERT INTO sessions (id, user_id, refresh_token_hash, user_agent, ip_address, is_valid, expires_at, last_used_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (id) DO UPDATE SET
			refresh_token_hash = EXCLUDED.refresh_token_hash,
			last_used_at = EXCLUDED.last_used_at
	`
	_, err := r.db.Exec(ctx, query,
		session.ID,
		session.UserID,
		session.RefreshTokenHash,
		session.UserAgent,
		session.IPAddress,
		session.IsValid,
		session.ExpiresAt,
		session.LastUsedAt,
	)
	return err
}

// GetSession retrieves a session by ID
func (r *UserRepository) GetSession(ctx context.Context, sessionID string) (*models.Session, error) {
	query := `
		SELECT id, user_id, refresh_token_hash, user_agent, ip_address, is_valid, 
		       created_at, expires_at, last_used_at, revoked_at
		FROM sessions
		WHERE id = $1
	`
	session := &models.Session{}
	// Use a pointer for revoked_at to safely handle NULL values
	var revokedAt *time.Time
	err := r.db.QueryRow(ctx, query, sessionID).Scan(
		&session.ID,
		&session.UserID,
		&session.RefreshTokenHash,
		&session.UserAgent,
		&session.IPAddress,
		&session.IsValid,
		&session.CreatedAt,
		&session.ExpiresAt,
		&session.LastUsedAt,
		&revokedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get session: %w", err)
	}
	// Assign the pointer - will be nil if NULL in database
	session.RevokedAt = revokedAt
	return session, nil
}

// InvalidateSession marks a session as invalid
func (r *UserRepository) InvalidateSession(ctx context.Context, sessionID string) error {
	query := `
		UPDATE sessions
		SET is_valid = false, revoked_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`
	_, err := r.db.Exec(ctx, query, sessionID)
	return err
}

// InvalidateAllUserSessions invalidates all sessions for a user
func (r *UserRepository) InvalidateAllUserSessions(ctx context.Context, userID string) error {
	query := `
		UPDATE sessions
		SET is_valid = false, revoked_at = CURRENT_TIMESTAMP
		WHERE user_id = $1 AND is_valid = true
	`
	_, err := r.db.Exec(ctx, query, userID)
	return err
}

// Token Blacklist

// BlacklistToken adds a token to the blacklist
func (r *UserRepository) BlacklistToken(ctx context.Context, tokenHash, userID string, expiresAt time.Time) error {
	query := `
		INSERT INTO token_blacklist (token_hash, user_id, expires_at, reason)
		VALUES ($1, $2, $3, 'logout')
		ON CONFLICT (token_hash) DO NOTHING
	`
	_, err := r.db.Exec(ctx, query, tokenHash, userID, expiresAt)
	return err
}

// IsTokenBlacklisted reports whether a token is blacklisted. The error is
// returned so callers can fail closed: a swallowed DB error here previously
// meant a revoked token was treated as valid (fail-open).
func (r *UserRepository) IsTokenBlacklisted(ctx context.Context, tokenHash string) (bool, error) {
	query := `
		SELECT EXISTS(SELECT 1 FROM token_blacklist WHERE token_hash = $1 AND expires_at > NOW())
	`
	var exists bool
	if err := r.db.QueryRow(ctx, query, tokenHash).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

// Password Reset Tokens

// CreatePasswordResetToken creates a new password reset token
func (r *UserRepository) CreatePasswordResetToken(ctx context.Context, token *models.PasswordResetToken) error {
	// First, invalidate any existing unused tokens for this user
	_, _ = r.db.Exec(ctx,
		"UPDATE password_reset_tokens SET used_at = CURRENT_TIMESTAMP WHERE user_id = $1 AND used_at IS NULL",
		token.UserID,
	)

	query := `
		INSERT INTO password_reset_tokens (id, user_id, token_hash, expires_at)
		VALUES ($1, $2, $3, $4)
	`
	_, err := r.db.Exec(ctx, query, token.ID, token.UserID, token.TokenHash, token.ExpiresAt)
	return err
}

// GetPasswordResetToken retrieves a password reset token by hash
func (r *UserRepository) GetPasswordResetToken(ctx context.Context, tokenHash string) (*models.PasswordResetToken, error) {
	query := `
		SELECT id, user_id, token_hash, expires_at, used_at, created_at
		FROM password_reset_tokens
		WHERE token_hash = $1
	`
	token := &models.PasswordResetToken{}
	err := r.db.QueryRow(ctx, query, tokenHash).Scan(
		&token.ID,
		&token.UserID,
		&token.TokenHash,
		&token.ExpiresAt,
		&token.UsedAt,
		&token.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get password reset token: %w", err)
	}
	return token, nil
}

// MarkResetTokenUsed marks a password reset token as used
func (r *UserRepository) MarkResetTokenUsed(ctx context.Context, tokenID string, usedAt time.Time) error {
	query := `
		UPDATE password_reset_tokens
		SET used_at = $2
		WHERE id = $1
	`
	_, err := r.db.Exec(ctx, query, tokenID, usedAt)
	return err
}

// 2FA Management

// StorePending2FASecret stores a pending 2FA secret during setup
func (r *UserRepository) StorePending2FASecret(ctx context.Context, userID string, secret string, backupCodes []string) error {
	// Store temporarily in the user record (not enabled yet)
	query := `
		UPDATE users
		SET two_factor_secret = $2, two_factor_backup_codes = $3, updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`
	_, err := r.db.Exec(ctx, query, userID, secret, pq.Array(backupCodes))
	return err
}

// GetPending2FASecret retrieves pending 2FA secret
func (r *UserRepository) GetPending2FASecret(ctx context.Context, userID string) (string, []string, error) {
	query := `
		SELECT two_factor_secret, two_factor_backup_codes
		FROM users
		WHERE id = $1 AND two_factor_enabled = false AND two_factor_secret IS NOT NULL
	`
	var secret string
	var backupCodes []string
	err := r.db.QueryRow(ctx, query, userID).Scan(&secret, pq.Array(&backupCodes))
	if err != nil {
		return "", nil, err
	}
	return secret, backupCodes, nil
}

// Enable2FA enables 2FA for a user
func (r *UserRepository) Enable2FA(ctx context.Context, userID string, secret string, backupCodes []string) error {
	query := `
		UPDATE users
		SET two_factor_enabled = true, two_factor_secret = $2, two_factor_backup_codes = $3, updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`
	_, err := r.db.Exec(ctx, query, userID, secret, pq.Array(backupCodes))
	return err
}

// Disable2FA disables 2FA for a user
func (r *UserRepository) Disable2FA(ctx context.Context, userID string) error {
	query := `
		UPDATE users
		SET two_factor_enabled = false, two_factor_secret = NULL, two_factor_backup_codes = NULL, updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`
	_, err := r.db.Exec(ctx, query, userID)
	return err
}

// API Keys

// CreateAPIKey creates a new API key
func (r *UserRepository) CreateAPIKey(ctx context.Context, key *models.APIKey) error {
	query := `
		INSERT INTO api_keys (id, user_id, name, key_prefix, key_hash, scopes, rate_limit, is_active, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`
	_, err := r.db.Exec(ctx, query,
		key.ID,
		key.UserID,
		key.Name,
		key.KeyPrefix,
		key.KeyHash,
		pq.Array(key.Scopes),
		key.RateLimit,
		key.IsActive,
		key.ExpiresAt,
	)
	return err
}

// GetAPIKey retrieves an API key by ID
func (r *UserRepository) GetAPIKey(ctx context.Context, keyID string) (*models.APIKey, error) {
	query := `
		SELECT id, user_id, name, key_prefix, key_hash, scopes, rate_limit, is_active, 
		       last_used_at, expires_at, created_at, revoked_at
		FROM api_keys
		WHERE id = $1
	`
	key := &models.APIKey{}
	err := r.db.QueryRow(ctx, query, keyID).Scan(
		&key.ID,
		&key.UserID,
		&key.Name,
		&key.KeyPrefix,
		&key.KeyHash,
		pq.Array(&key.Scopes),
		&key.RateLimit,
		&key.IsActive,
		&key.LastUsedAt,
		&key.ExpiresAt,
		&key.CreatedAt,
		&key.RevokedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get API key: %w", err)
	}
	return key, nil
}

// GetAPIKeyByHash retrieves an API key by its hash
func (r *UserRepository) GetAPIKeyByHash(ctx context.Context, keyHash string) (*models.APIKey, error) {
	query := `
		SELECT id, user_id, name, key_prefix, key_hash, scopes, rate_limit, is_active, 
		       last_used_at, expires_at, created_at, revoked_at
		FROM api_keys
		WHERE key_hash = $1 AND is_active = true AND (expires_at IS NULL OR expires_at > NOW())
	`
	key := &models.APIKey{}
	err := r.db.QueryRow(ctx, query, keyHash).Scan(
		&key.ID,
		&key.UserID,
		&key.Name,
		&key.KeyPrefix,
		&key.KeyHash,
		pq.Array(&key.Scopes),
		&key.RateLimit,
		&key.IsActive,
		&key.LastUsedAt,
		&key.ExpiresAt,
		&key.CreatedAt,
		&key.RevokedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get API key: %w", err)
	}
	return key, nil
}

// ListAPIKeys lists all API keys for a user
func (r *UserRepository) ListAPIKeys(ctx context.Context, userID string) ([]models.APIKey, error) {
	query := `
		SELECT id, user_id, name, key_prefix, scopes, rate_limit, is_active, 
		       last_used_at, expires_at, created_at, revoked_at
		FROM api_keys
		WHERE user_id = $1
		ORDER BY created_at DESC
	`
	rows, err := r.db.Query(ctx, query, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to list API keys: %w", err)
	}
	defer rows.Close()

	var keys []models.APIKey
	for rows.Next() {
		var key models.APIKey
		err := rows.Scan(
			&key.ID,
			&key.UserID,
			&key.Name,
			&key.KeyPrefix,
			pq.Array(&key.Scopes),
			&key.RateLimit,
			&key.IsActive,
			&key.LastUsedAt,
			&key.ExpiresAt,
			&key.CreatedAt,
			&key.RevokedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan API key: %w", err)
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// RevokeAPIKey revokes an API key
func (r *UserRepository) RevokeAPIKey(ctx context.Context, keyID string) error {
	query := `
		UPDATE api_keys
		SET is_active = false, revoked_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`
	_, err := r.db.Exec(ctx, query, keyID)
	return err
}

// UpdateAPIKeyLastUsed updates the last used timestamp
func (r *UserRepository) UpdateAPIKeyLastUsed(ctx context.Context, keyID string) error {
	query := `
		UPDATE api_keys
		SET last_used_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`
	_, err := r.db.Exec(ctx, query, keyID)
	return err
}

// Notifications

// CreateNotification creates a new notification
func (r *UserRepository) CreateNotification(ctx context.Context, notification *models.Notification) error {
	query := `
		INSERT INTO notifications (id, user_id, type, title, message, data, is_read)
		VALUES ($1, $2, $3, $4, $5, $6, false)
	`
	_, err := r.db.Exec(ctx, query,
		notification.ID,
		notification.UserID,
		notification.Type,
		notification.Title,
		notification.Message,
		notification.Data,
	)
	return err
}

// ListNotifications lists notifications for a user with pagination
func (r *UserRepository) ListNotifications(ctx context.Context, userID string, page, perPage int, unreadOnly bool) ([]models.Notification, int64, error) {
	// Build query based on filters
	whereClause := "WHERE user_id = $1"
	if unreadOnly {
		whereClause += " AND is_read = false"
	}

	// Get total count
	var total int64
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM notifications %s", whereClause)
	err := r.db.QueryRow(ctx, countQuery, userID).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count notifications: %w", err)
	}

	// Get notifications
	offset := (page - 1) * perPage
	query := fmt.Sprintf(`
		SELECT id, user_id, type, title, message, data, is_read, read_at, created_at
		FROM notifications
		%s
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`, whereClause)

	rows, err := r.db.Query(ctx, query, userID, perPage, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list notifications: %w", err)
	}
	defer rows.Close()

	var notifications []models.Notification
	for rows.Next() {
		var n models.Notification
		err := rows.Scan(
			&n.ID,
			&n.UserID,
			&n.Type,
			&n.Title,
			&n.Message,
			&n.Data,
			&n.IsRead,
			&n.ReadAt,
			&n.CreatedAt,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan notification: %w", err)
		}
		notifications = append(notifications, n)
	}
	return notifications, total, nil
}

// GetUnreadNotificationCount gets count of unread notifications
func (r *UserRepository) GetUnreadNotificationCount(ctx context.Context, userID string) (int64, error) {
	query := `SELECT COUNT(*) FROM notifications WHERE user_id = $1 AND is_read = false`
	var count int64
	err := r.db.QueryRow(ctx, query, userID).Scan(&count)
	return count, err
}

// MarkNotificationsRead marks notifications as read
func (r *UserRepository) MarkNotificationsRead(ctx context.Context, userID string, notificationIDs []string) (int64, error) {
	query := `
		UPDATE notifications
		SET is_read = true, read_at = CURRENT_TIMESTAMP
		WHERE user_id = $1 AND id = ANY($2) AND is_read = false
	`
	result, err := r.db.Exec(ctx, query, userID, pq.Array(notificationIDs))
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

// Security Events

// LogSecurityEvent logs a security event
func (r *UserRepository) LogSecurityEvent(ctx context.Context, event *models.SecurityEvent) error {
	query := `
		INSERT INTO security_events (user_id, event_type, success, ip_address, user_agent, location, details)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`
	_, err := r.db.Exec(ctx, query,
		event.UserID,
		event.EventType,
		event.Success,
		event.IPAddress,
		event.UserAgent,
		event.Location,
		event.Details,
	)
	return err
}

// GetRecentSecurityEvents gets recent security events for a user
func (r *UserRepository) GetRecentSecurityEvents(ctx context.Context, userID string, limit int) ([]models.SecurityEvent, error) {
	query := `
		SELECT id, user_id, event_type, success, ip_address, user_agent, location, details, created_at
		FROM security_events
		WHERE user_id = $1
		ORDER BY created_at DESC
		LIMIT $2
	`
	rows, err := r.db.Query(ctx, query, userID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get security events: %w", err)
	}
	defer rows.Close()

	var events []models.SecurityEvent
	for rows.Next() {
		var e models.SecurityEvent
		err := rows.Scan(
			&e.ID,
			&e.UserID,
			&e.EventType,
			&e.Success,
			&e.IPAddress,
			&e.UserAgent,
			&e.Location,
			&e.Details,
			&e.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan security event: %w", err)
		}
		events = append(events, e)
	}
	return events, nil
}

// Helper function to get DB pool for repository initialization
func NewUserRepositoryWithPool(db *pgxpool.Pool) *UserRepository {
	return &UserRepository{db: db}
}
