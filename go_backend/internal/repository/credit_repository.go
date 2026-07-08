package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tafolabi009/backend/go_backend/internal/models"
)

// ErrInsufficientCredits is returned when a charge cannot be covered by the
// user's balance. Handlers map it to 402 INSUFFICIENT_CREDITS.
var ErrInsufficientCredits = errors.New("insufficient credits")

type CreditRepository struct {
	db *pgxpool.Pool
}

func NewCreditRepository(db *pgxpool.Pool) *CreditRepository {
	return &CreditRepository{db: db}
}

// GetOrCreateBalance returns the credit balance for a user, creating one if it doesn't exist
func (r *CreditRepository) GetOrCreateBalance(ctx context.Context, userID string) (*models.CreditBalance, error) {
	balance := &models.CreditBalance{}
	err := r.db.QueryRow(ctx,
		`INSERT INTO credit_balances (id, user_id, balance, lifetime_purchased, lifetime_used)
		 VALUES ($1, $2, 0, 0, 0)
		 ON CONFLICT (user_id) DO UPDATE SET updated_at = CURRENT_TIMESTAMP
		 RETURNING id, user_id, balance, lifetime_purchased, lifetime_used, created_at, updated_at`,
		"cb_"+userID, userID,
	).Scan(&balance.ID, &balance.UserID, &balance.Balance, &balance.LifetimePurchased,
		&balance.LifetimeUsed, &balance.CreatedAt, &balance.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to get or create credit balance: %w", err)
	}
	return balance, nil
}

// GetBalance returns the current credit balance for a user
func (r *CreditRepository) GetBalance(ctx context.Context, userID string) (*models.CreditBalance, error) {
	balance := &models.CreditBalance{}
	err := r.db.QueryRow(ctx,
		`SELECT id, user_id, balance, lifetime_purchased, lifetime_used, created_at, updated_at
		 FROM credit_balances WHERE user_id = $1`,
		userID,
	).Scan(&balance.ID, &balance.UserID, &balance.Balance, &balance.LifetimePurchased,
		&balance.LifetimeUsed, &balance.CreatedAt, &balance.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to get credit balance: %w", err)
	}
	return balance, nil
}

// AddCredits adds credits to a user's balance and records a transaction
func (r *CreditRepository) AddCredits(ctx context.Context, userID string, amount int64, txType string, description string, refType *string, refID *string) (*models.CreditTransaction, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Update balance atomically
	var newBalance int64
	err = tx.QueryRow(ctx,
		`INSERT INTO credit_balances (id, user_id, balance, lifetime_purchased, lifetime_used)
		 VALUES ($1, $2, $3, $3, 0)
		 ON CONFLICT (user_id) DO UPDATE
		 SET balance = credit_balances.balance + $3,
		     lifetime_purchased = credit_balances.lifetime_purchased + $3,
		     updated_at = CURRENT_TIMESTAMP
		 RETURNING balance`,
		"cb_"+userID, userID, amount,
	).Scan(&newBalance)
	if err != nil {
		return nil, fmt.Errorf("failed to update credit balance: %w", err)
	}

	// Record transaction
	txID := "ctx_" + uuid.New().String()[:12]
	transaction := &models.CreditTransaction{}
	err = tx.QueryRow(ctx,
		`INSERT INTO credit_transactions (id, user_id, type, amount, balance_after, description, reference_type, reference_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 RETURNING id, user_id, type, amount, balance_after, description, reference_type, reference_id, created_at`,
		txID, userID, txType, amount, newBalance, description, refType, refID,
	).Scan(&transaction.ID, &transaction.UserID, &transaction.Type, &transaction.Amount,
		&transaction.BalanceAfter, &transaction.Description, &transaction.ReferenceType,
		&transaction.ReferenceID, &transaction.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to record credit transaction: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return transaction, nil
}

// DeductCredits deducts credits from a user's balance. Returns error if insufficient balance.
func (r *CreditRepository) DeductCredits(ctx context.Context, userID string, amount int64, description string, refType *string, refID *string) (*models.CreditTransaction, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Check and update balance atomically
	var newBalance int64
	err = tx.QueryRow(ctx,
		`UPDATE credit_balances
		 SET balance = balance - $2,
		     lifetime_used = lifetime_used + $2,
		     updated_at = CURRENT_TIMESTAMP
		 WHERE user_id = $1 AND balance >= $2
		 RETURNING balance`,
		userID, amount,
	).Scan(&newBalance)
	if err != nil {
		return nil, fmt.Errorf("insufficient credits or balance not found: %w", err)
	}

	// Record transaction
	txID := "ctx_" + uuid.New().String()[:12]
	transaction := &models.CreditTransaction{}
	err = tx.QueryRow(ctx,
		`INSERT INTO credit_transactions (id, user_id, type, amount, balance_after, description, reference_type, reference_id)
		 VALUES ($1, $2, 'deduction', $3, $4, $5, $6, $7)
		 RETURNING id, user_id, type, amount, balance_after, description, reference_type, reference_id, created_at`,
		txID, userID, -amount, newBalance, description, refType, refID,
	).Scan(&transaction.ID, &transaction.UserID, &transaction.Type, &transaction.Amount,
		&transaction.BalanceAfter, &transaction.Description, &transaction.ReferenceType,
		&transaction.ReferenceID, &transaction.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to record deduction transaction: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return transaction, nil
}

// ListTransactions returns paginated transaction history for a user
func (r *CreditRepository) ListTransactions(ctx context.Context, userID string, page, pageSize int) ([]models.CreditTransaction, int, error) {
	// Get total count
	var totalCount int
	err := r.db.QueryRow(ctx,
		`SELECT COUNT(*) FROM credit_transactions WHERE user_id = $1`,
		userID,
	).Scan(&totalCount)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count transactions: %w", err)
	}

	offset := (page - 1) * pageSize
	rows, err := r.db.Query(ctx,
		`SELECT id, user_id, type, amount, balance_after, description, reference_type, reference_id, metadata, created_at
		 FROM credit_transactions
		 WHERE user_id = $1
		 ORDER BY created_at DESC
		 LIMIT $2 OFFSET $3`,
		userID, pageSize, offset,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list transactions: %w", err)
	}
	defer rows.Close()

	transactions := []models.CreditTransaction{}
	for rows.Next() {
		var t models.CreditTransaction
		var metadataBytes []byte
		err := rows.Scan(&t.ID, &t.UserID, &t.Type, &t.Amount, &t.BalanceAfter,
			&t.Description, &t.ReferenceType, &t.ReferenceID, &metadataBytes, &t.CreatedAt)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan transaction: %w", err)
		}
		if metadataBytes != nil {
			json.Unmarshal(metadataBytes, &t.Metadata)
		}
		// Paddle hosted receipt link, when the payment webhook recorded one.
		if t.Metadata != nil {
			if r, ok := t.Metadata["receipt_url"].(string); ok && r != "" {
				t.ReceiptURL = &r
			}
		}
		transactions = append(transactions, t)
	}

	return transactions, totalCount, nil
}

// GetPackages returns all active credit packages
func (r *CreditRepository) GetPackages(ctx context.Context) ([]models.CreditPackage, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id, name, description, credits, price_cents, currency, bonus_credits, paddle_price_id, is_active, sort_order, created_at, updated_at
		 FROM credit_packages
		 WHERE is_active = true
		 ORDER BY sort_order ASC`,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list packages: %w", err)
	}
	defer rows.Close()

	packages := []models.CreditPackage{}
	for rows.Next() {
		var p models.CreditPackage
		err := rows.Scan(&p.ID, &p.Name, &p.Description, &p.Credits, &p.PriceCents,
			&p.Currency, &p.BonusCredits, &p.PaddlePriceID, &p.IsActive, &p.SortOrder, &p.CreatedAt, &p.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan package: %w", err)
		}
		packages = append(packages, p)
	}

	return packages, nil
}

// GetPackageByID returns a specific credit package
func (r *CreditRepository) GetPackageByID(ctx context.Context, packageID string) (*models.CreditPackage, error) {
	p := &models.CreditPackage{}
	err := r.db.QueryRow(ctx,
		`SELECT id, name, description, credits, price_cents, currency, bonus_credits, paddle_price_id, is_active, sort_order, created_at, updated_at
		 FROM credit_packages WHERE id = $1 AND is_active = true`,
		packageID,
	).Scan(&p.ID, &p.Name, &p.Description, &p.Credits, &p.PriceCents,
		&p.Currency, &p.BonusCredits, &p.PaddlePriceID, &p.IsActive, &p.SortOrder, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to get package: %w", err)
	}
	return p, nil
}

// GetCreditCosts returns all active credit costs
func (r *CreditRepository) GetCreditCosts(ctx context.Context) ([]models.CreditCost, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id, operation, credits_required, description, is_active, created_at, updated_at
		 FROM credit_costs WHERE is_active = true`,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list credit costs: %w", err)
	}
	defer rows.Close()

	costs := []models.CreditCost{}
	for rows.Next() {
		var c models.CreditCost
		err := rows.Scan(&c.ID, &c.Operation, &c.CreditsRequired, &c.Description, &c.IsActive, &c.CreatedAt, &c.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan credit cost: %w", err)
		}
		costs = append(costs, c)
	}

	return costs, nil
}

// CreateValidationCharged atomically deducts credits AND inserts the
// validation row in one transaction, so a crash between the two can never
// leave a charge without a job (or a free job without a charge).
// Returns ErrInsufficientCredits when the balance cannot cover the amount.
func (r *CreditRepository) CreateValidationCharged(ctx context.Context, v *models.Validation, validationType, priority string, amount int64, description string) (*models.CreditTransaction, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// 1) Deduct, guarded by balance >= amount (same semantics as DeductCredits).
	var newBalance int64
	err = tx.QueryRow(ctx,
		`UPDATE credit_balances
		 SET balance = balance - $2,
		     lifetime_used = lifetime_used + $2,
		     updated_at = CURRENT_TIMESTAMP
		 WHERE user_id = $1 AND balance >= $2
		 RETURNING balance`,
		v.UserID, amount,
	).Scan(&newBalance)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrInsufficientCredits
		}
		return nil, fmt.Errorf("failed to update credit balance: %w", err)
	}

	// 2) Record the deduction.
	refType := "validation"
	txID := "ctx_" + uuid.New().String()[:12]
	transaction := &models.CreditTransaction{}
	err = tx.QueryRow(ctx,
		`INSERT INTO credit_transactions (id, user_id, type, amount, balance_after, description, reference_type, reference_id)
		 VALUES ($1, $2, 'deduction', $3, $4, $5, $6, $7)
		 RETURNING id, user_id, type, amount, balance_after, description, reference_type, reference_id, created_at`,
		txID, v.UserID, -amount, newBalance, description, &refType, &v.ID,
	).Scan(&transaction.ID, &transaction.UserID, &transaction.Type, &transaction.Amount,
		&transaction.BalanceAfter, &transaction.Description, &transaction.ReferenceType,
		&transaction.ReferenceID, &transaction.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to record deduction transaction: %w", err)
	}

	// 3) Create the validation row itself.
	err = tx.QueryRow(ctx,
		`INSERT INTO validations (id, dataset_id, user_id, status, priority, validation_type, estimated_completion)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 RETURNING created_at`,
		v.ID, v.DatasetID, v.UserID, v.Status, priority, validationType, v.EstimatedCompletion,
	).Scan(&v.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to create validation: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}
	return transaction, nil
}

// RefundValidationCharge refunds the deduction recorded against referenceID
// (validation ID) if one exists and no refund has been issued yet. Idempotent:
// safe to call from cancel, failure reconciliation, and retries concurrently —
// the partial unique index uq_refund_per_reference plus the in-tx EXISTS guard
// ensure at most one refund is ever recorded per reference.
// Returns (amount refunded, true) when this call performed the refund.
func (r *CreditRepository) RefundValidationCharge(ctx context.Context, referenceID, userID, reason string) (int64, bool, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return 0, false, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Find what was charged.
	var amount int64
	err = tx.QueryRow(ctx,
		`SELECT ABS(amount) FROM credit_transactions
		 WHERE reference_id = $1 AND type = 'deduction'
		 ORDER BY created_at ASC LIMIT 1`,
		referenceID,
	).Scan(&amount)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, false, nil // nothing was charged; nothing to refund
		}
		return 0, false, fmt.Errorf("failed to look up charge: %w", err)
	}

	// Guard against an existing refund (fallback for installs where the
	// unique index could not be created over legacy duplicates).
	var alreadyRefunded bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM credit_transactions WHERE reference_id = $1 AND type = 'refund')`,
		referenceID,
	).Scan(&alreadyRefunded); err != nil {
		return 0, false, fmt.Errorf("failed to check refund state: %w", err)
	}
	if alreadyRefunded {
		return 0, false, nil
	}

	var newBalance int64
	err = tx.QueryRow(ctx,
		`INSERT INTO credit_balances (id, user_id, balance, lifetime_purchased, lifetime_used)
		 VALUES ($1, $2, $3, 0, 0)
		 ON CONFLICT (user_id) DO UPDATE
		 SET balance = credit_balances.balance + $3,
		     updated_at = CURRENT_TIMESTAMP
		 RETURNING balance`,
		"cb_"+userID, userID, amount,
	).Scan(&newBalance)
	if err != nil {
		return 0, false, fmt.Errorf("failed to update credit balance: %w", err)
	}

	refType := "validation_refund"
	txID := "ctx_" + uuid.New().String()[:12]
	if _, err := tx.Exec(ctx,
		`INSERT INTO credit_transactions (id, user_id, type, amount, balance_after, description, reference_type, reference_id)
		 VALUES ($1, $2, 'refund', $3, $4, $5, $6, $7)`,
		txID, userID, amount, newBalance, reason, &refType, &referenceID,
	); err != nil {
		// Unique-index violation here means a concurrent refund won the race.
		return 0, false, nil
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, false, fmt.Errorf("failed to commit refund: %w", err)
	}
	return amount, true, nil
}

// GetCreditCostByOperation returns the credit cost for a specific operation
func (r *CreditRepository) GetCreditCostByOperation(ctx context.Context, operation string) (*models.CreditCost, error) {
	c := &models.CreditCost{}
	err := r.db.QueryRow(ctx,
		`SELECT id, operation, credits_required, description, is_active, created_at, updated_at
		 FROM credit_costs WHERE operation = $1 AND is_active = true`,
		operation,
	).Scan(&c.ID, &c.Operation, &c.CreditsRequired, &c.Description, &c.IsActive, &c.CreatedAt, &c.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to get credit cost for operation %s: %w", operation, err)
	}
	return c, nil
}
