package database

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var db *pgxpool.Pool

// Init initializes the database connection pool
func Init(databaseURL string) error {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return fmt.Errorf("unable to parse database URL: %w", err)
	}

	// Connection pool settings
	config.MaxConns = 25
	config.MinConns = 5
	config.MaxConnLifetime = time.Hour
	config.MaxConnIdleTime = 30 * time.Minute
	config.HealthCheckPeriod = time.Minute

	// Create connection pool
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return fmt.Errorf("unable to create connection pool: %w", err)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("unable to ping database: %w", err)
	}

	db = pool
	log.Println("✅ Database connection established")

	// Run auto-migrations - fail fast if schema is invalid
	if err := runMigrations(pool); err != nil {
		pool.Close()
		return fmt.Errorf("database migration failed: %w", err)
	}

	return nil
}

// runMigrations creates tables if they don't exist
func runMigrations(pool *pgxpool.Pool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	migrations := []string{
		// Users table
		`CREATE TABLE IF NOT EXISTS users (
			id VARCHAR(255) PRIMARY KEY,
			email VARCHAR(255) UNIQUE NOT NULL,
			username VARCHAR(100) UNIQUE,
			password_hash VARCHAR(255) NOT NULL,
			full_name VARCHAR(255),
			company_id VARCHAR(255),
			company_name VARCHAR(255),
			role VARCHAR(50) DEFAULT 'user',
			subscription_tier VARCHAR(50) DEFAULT 'free',
			api_key VARCHAR(255) UNIQUE,
			rate_limit_tier VARCHAR(50) DEFAULT 'standard',
			two_factor_enabled BOOLEAN DEFAULT false,
			two_factor_secret VARCHAR(255),
			two_factor_backup_codes TEXT[],
			failed_login_attempts INT DEFAULT 0,
			locked_until TIMESTAMP,
			password_changed_at TIMESTAMP,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			last_login_at TIMESTAMP,
			is_active BOOLEAN DEFAULT true,
			email_verified BOOLEAN DEFAULT false
		)`,
		// Add columns for existing tables
		`ALTER TABLE users ADD COLUMN IF NOT EXISTS username VARCHAR(100) UNIQUE`,
		`ALTER TABLE users ADD COLUMN IF NOT EXISTS role VARCHAR(50) DEFAULT 'user'`,
		`ALTER TABLE users ADD COLUMN IF NOT EXISTS two_factor_enabled BOOLEAN DEFAULT false`,
		`ALTER TABLE users ADD COLUMN IF NOT EXISTS two_factor_secret VARCHAR(255)`,
		`ALTER TABLE users ADD COLUMN IF NOT EXISTS two_factor_backup_codes TEXT[]`,
		`ALTER TABLE users ADD COLUMN IF NOT EXISTS failed_login_attempts INT DEFAULT 0`,
		`ALTER TABLE users ADD COLUMN IF NOT EXISTS locked_until TIMESTAMP`,
		`ALTER TABLE users ADD COLUMN IF NOT EXISTS password_changed_at TIMESTAMP`,
		`ALTER TABLE users ADD COLUMN IF NOT EXISTS job_title VARCHAR(100)`,
		`ALTER TABLE users ADD COLUMN IF NOT EXISTS phone VARCHAR(20)`,
		`CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)`,
		`CREATE INDEX IF NOT EXISTS idx_users_company_id ON users(company_id)`,
		`CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)`,
		`CREATE INDEX IF NOT EXISTS idx_users_role ON users(role)`,

		// Sessions table for session management
		`CREATE TABLE IF NOT EXISTS sessions (
			id VARCHAR(255) PRIMARY KEY,
			user_id VARCHAR(255) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			refresh_token_hash VARCHAR(255) NOT NULL,
			user_agent TEXT,
			ip_address VARCHAR(45),
			is_valid BOOLEAN DEFAULT true,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			expires_at TIMESTAMP NOT NULL,
			last_used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			revoked_at TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id)`,
		`CREATE INDEX IF NOT EXISTS idx_sessions_is_valid ON sessions(is_valid)`,

		// Token blacklist table
		`CREATE TABLE IF NOT EXISTS token_blacklist (
			id SERIAL PRIMARY KEY,
			token_hash VARCHAR(255) NOT NULL UNIQUE,
			user_id VARCHAR(255) NOT NULL,
			expires_at TIMESTAMP NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_token_blacklist_token_hash ON token_blacklist(token_hash)`,
		`CREATE INDEX IF NOT EXISTS idx_token_blacklist_expires_at ON token_blacklist(expires_at)`,

		// Notifications table
		`CREATE TABLE IF NOT EXISTS notifications (
			id VARCHAR(255) PRIMARY KEY,
			user_id VARCHAR(255) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			type VARCHAR(50) NOT NULL,
			title VARCHAR(255) NOT NULL,
			message TEXT NOT NULL,
			data JSONB,
			is_read BOOLEAN DEFAULT false,
			read_at TIMESTAMP,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_notifications_user_id ON notifications(user_id)`,
		`CREATE INDEX IF NOT EXISTS idx_notifications_is_read ON notifications(is_read)`,

		// API keys table
		`CREATE TABLE IF NOT EXISTS api_keys (
			id VARCHAR(255) PRIMARY KEY,
			user_id VARCHAR(255) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			name VARCHAR(255) NOT NULL,
			key_prefix VARCHAR(12) NOT NULL,
			key_hash VARCHAR(255) NOT NULL,
			scopes TEXT[] DEFAULT '{}',
			rate_limit INT DEFAULT 1000,
			is_active BOOLEAN DEFAULT true,
			last_used_at TIMESTAMP,
			expires_at TIMESTAMP,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			revoked_at TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_api_keys_user_id ON api_keys(user_id)`,
		`CREATE INDEX IF NOT EXISTS idx_api_keys_key_hash ON api_keys(key_hash)`,

		// Security events table
		`CREATE TABLE IF NOT EXISTS security_events (
			id SERIAL PRIMARY KEY,
			user_id VARCHAR(255),
			event_type VARCHAR(50) NOT NULL,
			success BOOLEAN NOT NULL,
			ip_address VARCHAR(45),
			user_agent TEXT,
			details JSONB,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_security_events_user_id ON security_events(user_id)`,
		`CREATE INDEX IF NOT EXISTS idx_security_events_event_type ON security_events(event_type)`,

		// Datasets table
		`CREATE TABLE IF NOT EXISTS datasets (
			id VARCHAR(255) PRIMARY KEY,
			user_id VARCHAR(255) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			filename VARCHAR(500) NOT NULL,
			file_size BIGINT NOT NULL,
			file_type VARCHAR(100) NOT NULL,
			storage_path VARCHAR(1000),
			upload_url TEXT,
			status VARCHAR(50) NOT NULL DEFAULT 'pending',
			format VARCHAR(50),
			row_count BIGINT,
			column_count INT,
			description TEXT,
			metadata JSONB,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			processed_at TIMESTAMP,
			deleted_at TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_datasets_user_id ON datasets(user_id)`,
		`CREATE INDEX IF NOT EXISTS idx_datasets_status ON datasets(status)`,

		// Validations table
		`CREATE TABLE IF NOT EXISTS validations (
			id VARCHAR(255) PRIMARY KEY,
			dataset_id VARCHAR(255) REFERENCES datasets(id) ON DELETE CASCADE,
			user_id VARCHAR(255) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			job_id VARCHAR(255),
			pipeline_id VARCHAR(255),
			status VARCHAR(50) NOT NULL DEFAULT 'queued',
			priority VARCHAR(20) DEFAULT 'standard',
			progress FLOAT DEFAULT 0,
			current_stage VARCHAR(100),
			estimated_completion TIMESTAMP,
			diversity_score FLOAT,
			validation_score FLOAT,
			collapse_detected BOOLEAN,
			collapse_severity VARCHAR(50),
			report_url TEXT,
			certificate_url TEXT,
			error_message TEXT,
			metadata JSONB,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			started_at TIMESTAMP,
			completed_at TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_validations_dataset_id ON validations(dataset_id)`,
		`CREATE INDEX IF NOT EXISTS idx_validations_user_id ON validations(user_id)`,
		`CREATE INDEX IF NOT EXISTS idx_validations_status ON validations(status)`,

		// Warranties table
		`CREATE TABLE IF NOT EXISTS warranties (
			id VARCHAR(255) PRIMARY KEY,
			validation_id VARCHAR(255) REFERENCES validations(id),
			user_id VARCHAR(255) NOT NULL REFERENCES users(id),
			status VARCHAR(50) NOT NULL DEFAULT 'pending',
			warranty_type VARCHAR(50),
			coverage_amount DECIMAL(12,2),
			start_date TIMESTAMP,
			end_date TIMESTAMP,
			terms TEXT,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			approved_at TIMESTAMP,
			rejected_at TIMESTAMP,
			rejection_reason TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS idx_warranties_user_id ON warranties(user_id)`,
		`CREATE INDEX IF NOT EXISTS idx_warranties_validation_id ON warranties(validation_id)`,

		// Warranty claims table
		`CREATE TABLE IF NOT EXISTS warranty_claims (
			id VARCHAR(255) PRIMARY KEY,
			warranty_id VARCHAR(255) NOT NULL REFERENCES warranties(id),
			user_id VARCHAR(255) NOT NULL REFERENCES users(id),
			claim_type VARCHAR(50),
			claim_amount DECIMAL(12,2),
			description TEXT,
			status VARCHAR(50) NOT NULL DEFAULT 'submitted',
			resolution TEXT,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			reviewed_at TIMESTAMP,
			resolved_at TIMESTAMP
		)`,

		// Add missing columns (for schema compatibility)
		`ALTER TABLE datasets ADD COLUMN IF NOT EXISTS s3_path VARCHAR(1000)`,
		`ALTER TABLE datasets ADD COLUMN IF NOT EXISTS uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`,
		`ALTER TABLE validations ADD COLUMN IF NOT EXISTS risk_score INT`,
		`ALTER TABLE validations ADD COLUMN IF NOT EXISTS risk_level VARCHAR(50)`,
		`ALTER TABLE validations ADD COLUMN IF NOT EXISTS recommendation TEXT`,
		`ALTER TABLE validations ADD COLUMN IF NOT EXISTS warranty_eligible BOOLEAN`,
		`ALTER TABLE validations ADD COLUMN IF NOT EXISTS validation_type VARCHAR(50) DEFAULT 'comprehensive'`,

		// Fix warranties table to match repository code
		`ALTER TABLE warranties ADD COLUMN IF NOT EXISTS warranty_type VARCHAR(50)`,
		`ALTER TABLE warranties ADD COLUMN IF NOT EXISTS coverage_amount DECIMAL(12,2)`,
		`ALTER TABLE warranties ADD COLUMN IF NOT EXISTS start_date TIMESTAMP`,
		`ALTER TABLE warranties ADD COLUMN IF NOT EXISTS end_date TIMESTAMP`,
		`ALTER TABLE warranties ADD COLUMN IF NOT EXISTS terms TEXT`,
		`ALTER TABLE warranties ADD COLUMN IF NOT EXISTS approved_at TIMESTAMP`,
		`ALTER TABLE warranties ADD COLUMN IF NOT EXISTS rejected_at TIMESTAMP`,
		`ALTER TABLE warranties ADD COLUMN IF NOT EXISTS rejection_reason TEXT`,

		// Fix warranty_claims table to match repository code
		`ALTER TABLE warranty_claims ADD COLUMN IF NOT EXISTS claim_type VARCHAR(50)`,
		`ALTER TABLE warranty_claims ADD COLUMN IF NOT EXISTS description TEXT`,
		`ALTER TABLE warranty_claims ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMP`,

		// Support tickets
		`CREATE TABLE IF NOT EXISTS support_tickets (
			id VARCHAR(36) PRIMARY KEY,
			user_id VARCHAR(36) NOT NULL REFERENCES users(id),
			assigned_to VARCHAR(36) REFERENCES users(id),
			subject VARCHAR(255) NOT NULL,
			category VARCHAR(50) DEFAULT 'general',
			priority VARCHAR(20) DEFAULT 'normal',
			status VARCHAR(20) DEFAULT 'open',
			created_at TIMESTAMPTZ DEFAULT NOW(),
			updated_at TIMESTAMPTZ DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_tickets_user_id ON support_tickets(user_id)`,
		`CREATE INDEX IF NOT EXISTS idx_tickets_status ON support_tickets(status)`,
		`CREATE INDEX IF NOT EXISTS idx_tickets_assigned ON support_tickets(assigned_to)`,

		// Ticket messages
		`CREATE TABLE IF NOT EXISTS ticket_messages (
			id VARCHAR(36) PRIMARY KEY,
			ticket_id VARCHAR(36) NOT NULL REFERENCES support_tickets(id) ON DELETE CASCADE,
			sender_id VARCHAR(36) NOT NULL REFERENCES users(id),
			message TEXT NOT NULL,
			is_internal BOOLEAN DEFAULT false,
			created_at TIMESTAMPTZ DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_ticket_messages_ticket ON ticket_messages(ticket_id)`,

		// Team invites
		`CREATE TABLE IF NOT EXISTS invites (
			id VARCHAR(36) PRIMARY KEY,
			email VARCHAR(255) NOT NULL,
			role VARCHAR(50) NOT NULL DEFAULT 'user',
			invited_by VARCHAR(36) NOT NULL REFERENCES users(id),
			token VARCHAR(255) NOT NULL UNIQUE,
			status VARCHAR(20) DEFAULT 'pending',
			expires_at TIMESTAMPTZ NOT NULL,
			created_at TIMESTAMPTZ DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_invites_token ON invites(token)`,
		`CREATE INDEX IF NOT EXISTS idx_invites_email ON invites(email)`,

		// Promo codes (ensure exists)
		`CREATE TABLE IF NOT EXISTS promo_codes (
			id VARCHAR(36) PRIMARY KEY,
			code VARCHAR(50) NOT NULL UNIQUE,
			credits_grant BIGINT NOT NULL DEFAULT 0,
			package_id VARCHAR(36),
			description TEXT DEFAULT '',
			max_uses INT DEFAULT 100,
			current_uses INT DEFAULT 0,
			is_active BOOLEAN DEFAULT true,
			created_at TIMESTAMPTZ DEFAULT NOW(),
			updated_at TIMESTAMPTZ DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_promo_codes_code ON promo_codes(code)`,

		// Promo redemptions (ensure exists with unique constraint)
		`CREATE TABLE IF NOT EXISTS promo_redemptions (
			id VARCHAR(36) PRIMARY KEY,
			promo_code_id VARCHAR(36) NOT NULL,
			user_id VARCHAR(36) NOT NULL,
			credits_granted BIGINT NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ DEFAULT NOW(),
			UNIQUE(promo_code_id, user_id)
		)`,

		// Email verifications table for OTP-based email verification
		`CREATE TABLE IF NOT EXISTS email_verifications (
			id VARCHAR(36) PRIMARY KEY,
			user_id VARCHAR(36) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			email VARCHAR(255) NOT NULL,
			otp_hash VARCHAR(255) NOT NULL,
			attempts INT DEFAULT 0,
			expires_at TIMESTAMPTZ NOT NULL,
			created_at TIMESTAMPTZ DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_email_verifications_user ON email_verifications(user_id)`,

		// Platform settings table
		`CREATE TABLE IF NOT EXISTS platform_settings (
			key VARCHAR(100) PRIMARY KEY,
			value JSONB NOT NULL DEFAULT '{}',
			updated_by VARCHAR(36),
			updated_at TIMESTAMPTZ DEFAULT NOW()
		)`,

		// Notification preferences table
		`CREATE TABLE IF NOT EXISTS notification_preferences (
			user_id VARCHAR(36) PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
			email_notifications BOOLEAN DEFAULT true,
			validation_complete BOOLEAN DEFAULT true,
			warranty_expiring BOOLEAN DEFAULT true,
			weekly_digest BOOLEAN DEFAULT false,
			ticket_updates BOOLEAN DEFAULT true,
			updated_at TIMESTAMPTZ DEFAULT NOW()
		)`,

		// Webhooks table
		`CREATE TABLE IF NOT EXISTS webhooks (
			id VARCHAR(36) PRIMARY KEY,
			user_id VARCHAR(36) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			url VARCHAR(1000) NOT NULL,
			secret VARCHAR(255) NOT NULL,
			events TEXT[] DEFAULT '{}',
			is_active BOOLEAN DEFAULT true,
			last_triggered_at TIMESTAMPTZ,
			failure_count INT DEFAULT 0,
			created_at TIMESTAMPTZ DEFAULT NOW(),
			updated_at TIMESTAMPTZ DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_webhooks_user ON webhooks(user_id)`,

		// Webhook deliveries table
		`CREATE TABLE IF NOT EXISTS webhook_deliveries (
			id VARCHAR(36) PRIMARY KEY,
			webhook_id VARCHAR(36) NOT NULL REFERENCES webhooks(id) ON DELETE CASCADE,
			event_type VARCHAR(100) NOT NULL,
			payload JSONB NOT NULL,
			response_status INT,
			response_body TEXT,
			success BOOLEAN DEFAULT false,
			duration_ms INT,
			created_at TIMESTAMPTZ DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_webhook ON webhook_deliveries(webhook_id)`,

		// Credit balances
		`CREATE TABLE IF NOT EXISTS credit_balances (
			id VARCHAR(36) PRIMARY KEY,
			user_id VARCHAR(255) NOT NULL UNIQUE REFERENCES users(id) ON DELETE CASCADE,
			balance BIGINT NOT NULL DEFAULT 0,
			lifetime_purchased BIGINT NOT NULL DEFAULT 0,
			lifetime_used BIGINT NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ DEFAULT NOW(),
			updated_at TIMESTAMPTZ DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_credit_balances_user ON credit_balances(user_id)`,

		// Credit transactions
		`CREATE TABLE IF NOT EXISTS credit_transactions (
			id VARCHAR(36) PRIMARY KEY,
			user_id VARCHAR(255) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			amount BIGINT NOT NULL,
			type VARCHAR(50) NOT NULL,
			description TEXT DEFAULT '',
			reference_type VARCHAR(50),
			reference_id VARCHAR(255),
			balance_after BIGINT NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_credit_transactions_user ON credit_transactions(user_id)`,

		// Credit packages
		`CREATE TABLE IF NOT EXISTS credit_packages (
			id VARCHAR(36) PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			description TEXT DEFAULT '',
			credits BIGINT NOT NULL DEFAULT 0,
			bonus_credits BIGINT NOT NULL DEFAULT 0,
			price_cents BIGINT NOT NULL DEFAULT 0,
			currency VARCHAR(10) DEFAULT 'USD',
			is_active BOOLEAN DEFAULT true,
			created_at TIMESTAMPTZ DEFAULT NOW(),
			updated_at TIMESTAMPTZ DEFAULT NOW()
		)`,

		// Credit costs
		`CREATE TABLE IF NOT EXISTS credit_costs (
			id VARCHAR(36) PRIMARY KEY,
			operation VARCHAR(100) NOT NULL UNIQUE,
			credits_required BIGINT NOT NULL DEFAULT 0,
			description TEXT DEFAULT '',
			is_active BOOLEAN DEFAULT true,
			created_at TIMESTAMPTZ DEFAULT NOW(),
			updated_at TIMESTAMPTZ DEFAULT NOW()
		)`,

		// Add location column to security_events
		`ALTER TABLE security_events ADD COLUMN IF NOT EXISTS location VARCHAR(100) DEFAULT ''`,

		// Credit schema drift fixes — columns the credit repository selects but
		// the original CREATE TABLE statements above omit (existing tables aren't
		// altered by CREATE TABLE IF NOT EXISTS, so add them explicitly).
		`ALTER TABLE credit_packages ADD COLUMN IF NOT EXISTS sort_order INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE credit_transactions ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}'::jsonb`,

		// Idempotency keys: safe client retries on charging endpoints. A stored
		// row replays the original successful response instead of re-charging.
		`CREATE TABLE IF NOT EXISTS idempotency_keys (
			user_id VARCHAR(255) NOT NULL,
			endpoint VARCHAR(100) NOT NULL,
			idem_key VARCHAR(255) NOT NULL,
			request_hash VARCHAR(64) NOT NULL,
			response_status INTEGER NOT NULL,
			response_body JSONB,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			expires_at TIMESTAMPTZ NOT NULL,
			PRIMARY KEY (user_id, endpoint, idem_key)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_idempotency_expires ON idempotency_keys(expires_at)`,

		// Calibration loop: user-reported downstream outcomes vs our predicted risk.
		`CREATE TABLE IF NOT EXISTS validation_outcomes (
			id VARCHAR(36) PRIMARY KEY,
			validation_id VARCHAR(255) NOT NULL UNIQUE REFERENCES validations(id) ON DELETE CASCADE,
			user_id VARCHAR(255) NOT NULL,
			predicted_risk INTEGER,
			outcome VARCHAR(20) NOT NULL,
			actual_metric DOUBLE PRECISION,
			notes TEXT,
			observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_validation_outcomes_user ON validation_outcomes(user_id)`,

		// Continuous drift monitoring: scheduled re-validation of a dataset.
		`CREATE TABLE IF NOT EXISTS dataset_monitors (
			id VARCHAR(255) PRIMARY KEY,
			user_id VARCHAR(255) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			dataset_id VARCHAR(255) NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
			name VARCHAR(255) NOT NULL,
			interval_hours INTEGER NOT NULL DEFAULT 24,
			max_risk_score INTEGER NOT NULL DEFAULT 50,
			validation_type VARCHAR(50) NOT NULL DEFAULT 'comprehensive',
			is_active BOOLEAN NOT NULL DEFAULT true,
			paused_reason TEXT,
			last_run_at TIMESTAMPTZ,
			next_run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			last_validation_id VARCHAR(255),
			last_risk_score INTEGER,
			consecutive_alerts INTEGER NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_dataset_monitors_user ON dataset_monitors(user_id)`,
		`CREATE INDEX IF NOT EXISTS idx_dataset_monitors_due ON dataset_monitors(is_active, next_run_at)`,

		`CREATE TABLE IF NOT EXISTS monitor_runs (
			id VARCHAR(36) PRIMARY KEY,
			monitor_id VARCHAR(255) NOT NULL REFERENCES dataset_monitors(id) ON DELETE CASCADE,
			validation_id VARCHAR(255) NOT NULL,
			status VARCHAR(20) NOT NULL DEFAULT 'pending',
			risk_score INTEGER,
			alerted BOOLEAN NOT NULL DEFAULT false,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			evaluated_at TIMESTAMPTZ
		)`,
		`CREATE INDEX IF NOT EXISTS idx_monitor_runs_monitor ON monitor_runs(monitor_id, created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_monitor_runs_pending ON monitor_runs(status)`,

		// Shareable read-only report links.
		`CREATE TABLE IF NOT EXISTS report_shares (
			token VARCHAR(64) PRIMARY KEY,
			user_id VARCHAR(255) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			validation_id VARCHAR(255) NOT NULL REFERENCES validations(id) ON DELETE CASCADE,
			expires_at TIMESTAMPTZ NOT NULL,
			revoked BOOLEAN NOT NULL DEFAULT false,
			view_count INTEGER NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_report_shares_validation ON report_shares(validation_id)`,

		// Certificate signing keypair (Ed25519), shared across gateway instances.
		`CREATE TABLE IF NOT EXISTS signing_keys (
			id VARCHAR(50) PRIMARY KEY,
			algorithm VARCHAR(20) NOT NULL DEFAULT 'Ed25519',
			public_key TEXT NOT NULL,
			private_key TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,

		// Per-user quota overrides; defaults come from env when no row exists.
		`CREATE TABLE IF NOT EXISTS user_quotas (
			user_id VARCHAR(255) PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
			max_dataset_bytes BIGINT,
			max_validations_per_day INTEGER,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,

		// Validations schema drift fixes — columns the handlers write but the
		// original CREATE TABLE omits (same pattern as the credit fixes above).
		`ALTER TABLE validations ADD COLUMN IF NOT EXISTS validation_type VARCHAR(50)`,
		`ALTER TABLE validations ADD COLUMN IF NOT EXISTS risk_score INTEGER`,
		`ALTER TABLE validations ADD COLUMN IF NOT EXISTS risk_level VARCHAR(20)`,
		`ALTER TABLE validations ADD COLUMN IF NOT EXISTS recommendation TEXT`,
		`ALTER TABLE validations ADD COLUMN IF NOT EXISTS warranty_eligible BOOLEAN`,

		// Per-stage pipeline timestamps for the live progress endpoint.
		`ALTER TABLE validations ADD COLUMN IF NOT EXISTS stage_history JSONB DEFAULT '[]'::jsonb`,

		// User-editable validation display name (rename feature).
		`ALTER TABLE validations ADD COLUMN IF NOT EXISTS name VARCHAR(120)`,

		// Rolling service health samples powering GET /health/uptime.
		`CREATE TABLE IF NOT EXISTS service_health_checks (
			id BIGSERIAL PRIMARY KEY,
			service VARCHAR(50) NOT NULL,
			healthy BOOLEAN NOT NULL,
			latency_ms INTEGER,
			checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_health_checks_service_time ON service_health_checks(service, checked_at DESC)`,

		// Dataset groups: validate a folder of files as one logical dataset.
		`CREATE TABLE IF NOT EXISTS dataset_groups (
			id VARCHAR(255) PRIMARY KEY,
			owner_id VARCHAR(255) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			name VARCHAR(255) NOT NULL,
			archived BOOLEAN NOT NULL DEFAULT false,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			UNIQUE (owner_id, name)
		)`,
		`ALTER TABLE datasets ADD COLUMN IF NOT EXISTS group_id VARCHAR(255)`,
		`CREATE INDEX IF NOT EXISTS idx_datasets_group ON datasets(group_id)`,
		`ALTER TABLE validations ADD COLUMN IF NOT EXISTS group_id VARCHAR(255)`,

		// Paddle catalog mapping for hosted checkout/receipts.
		`ALTER TABLE credit_packages ADD COLUMN IF NOT EXISTS paddle_price_id VARCHAR(255)`,

		// Auto-validate schedules; daily/weekly cadences are backed by a
		// dataset_monitor, on_upload hooks the upload-complete path.
		`CREATE TABLE IF NOT EXISTS dataset_schedules (
			dataset_id VARCHAR(255) PRIMARY KEY REFERENCES datasets(id) ON DELETE CASCADE,
			user_id VARCHAR(255) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			cadence VARCHAR(20) NOT NULL,
			validation_type VARCHAR(50) NOT NULL DEFAULT 'comprehensive',
			monitor_id VARCHAR(255),
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
	}

	for i, migration := range migrations {
		if _, err := pool.Exec(ctx, migration); err != nil {
			// Return immediately on migration failure - fail fast
			return fmt.Errorf("migration %d failed: %w", i+1, err)
		}
	}

	// Seed default platform settings
	_, _ = pool.Exec(ctx, `INSERT INTO platform_settings (key, value) VALUES
		('registration_enabled', 'true'),
		('maintenance_mode', 'false'),
		('max_upload_size_gb', '500'),
		('default_signup_credits', '0'),
		('allowed_email_domains', '""')
	ON CONFLICT (key) DO NOTHING`)

	// At most one refund per reference (validation/warranty/...). Best-effort:
	// if legacy data already contains duplicate refunds this index cannot be
	// created, and the code-level EXISTS guard remains the only protection.
	_, _ = pool.Exec(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS uq_refund_per_reference
		ON credit_transactions(reference_id) WHERE type = 'refund' AND reference_id IS NOT NULL`)

	// At most one credit grant per Paddle transaction (webhook idempotency).
	_, _ = pool.Exec(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS uq_paddle_txn
		ON credit_transactions(reference_id) WHERE reference_type = 'paddle' AND reference_id IS NOT NULL`)

	// Password reset tokens table
	_, _ = pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS password_reset_tokens (
		id VARCHAR(36) PRIMARY KEY,
		user_id VARCHAR(255) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		token_hash VARCHAR(255) NOT NULL,
		expires_at TIMESTAMPTZ NOT NULL,
		used_at TIMESTAMPTZ,
		created_at TIMESTAMPTZ DEFAULT NOW()
	)`)
	_, _ = pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_reset_tokens_user ON password_reset_tokens(user_id)`)

	// Admin promotion
	_, _ = pool.Exec(ctx, `UPDATE users SET role = 'admin' WHERE email = 'tafolabi009@gmail.com'`)
	// Seed credit packages
	_, _ = pool.Exec(ctx, `INSERT INTO credit_packages (id, name, description, credits, bonus_credits, price_cents, currency) VALUES
		('pkg_starter', 'Starter', '2 small-scale validations included', 50, 0, 150000, 'USD'),
		('pkg_professional', 'Professional', 'For growing teams with regular validation needs', 500, 100, 500000, 'USD'),
		('pkg_business', 'Business', 'High-volume enterprise validation workloads', 2500, 500, 2000000, 'USD'),
		('pkg_enterprise', 'Enterprise', 'Unlimited-scale validation with dedicated support and SLA', 15000, 5000, 8000000, 'USD')
	ON CONFLICT (id) DO NOTHING`)

	// Give seeded packages a sensible display order (sort_order defaults to 0)
	_, _ = pool.Exec(ctx, `UPDATE credit_packages SET sort_order = CASE id
		WHEN 'pkg_starter' THEN 1
		WHEN 'pkg_professional' THEN 2
		WHEN 'pkg_business' THEN 3
		WHEN 'pkg_enterprise' THEN 4
		ELSE sort_order END
	WHERE sort_order = 0`)

	// Seed credit costs
	_, _ = pool.Exec(ctx, `INSERT INTO credit_costs (id, operation, credits_required, description) VALUES
		('cost_val_std', 'validation_standard', 25, 'Standard priority validation job'),
		('cost_val_exp', 'validation_express', 50, 'Express priority validation job (2x)'),
		('cost_warranty', 'warranty_request', 15, 'Performance warranty request'),
		('cost_revalidation', 'revalidation', 20, 'Re-validation of previously validated dataset')
	ON CONFLICT (id) DO NOTHING`)

	// Fix stuck datasets - only fix datasets older than 1 hour
	_, _ = pool.Exec(ctx, `UPDATE datasets SET status = 'ready' WHERE status IN ('processing', 'uploading') AND updated_at < NOW() - INTERVAL '1 hour'`)

	log.Println("✅ Database migrations completed")
	return nil
}

// GetDB returns the database connection pool
func GetDB() *pgxpool.Pool {
	return db
}

// Close closes the database connection pool
func Close() {
	if db != nil {
		db.Close()
		log.Println("Database connection closed")
	}
}

// Health checks database health
func Health() error {
	if db == nil {
		return fmt.Errorf("database not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := db.Ping(ctx); err != nil {
		return fmt.Errorf("database ping failed: %w", err)
	}

	return nil
}

// IsHealthy returns true if database is healthy
func IsHealthy() bool {
	return Health() == nil
}

// Stats returns database pool statistics
func Stats() *pgxpool.Stat {
	if db == nil {
		return nil
	}
	return db.Stat()
}
