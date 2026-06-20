package repository

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tafolabi009/backend/go_backend/internal/models"
)

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
		transactions = append(transactions, t)
	}

	return transactions, totalCount, nil
}

// GetPackages returns all active credit packages
func (r *CreditRepository) GetPackages(ctx context.Context) ([]models.CreditPackage, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id, name, description, credits, price_cents, currency, bonus_credits, is_active, sort_order, created_at, updated_at
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
			&p.Currency, &p.BonusCredits, &p.IsActive, &p.SortOrder, &p.CreatedAt, &p.UpdatedAt)
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
		`SELECT id, name, description, credits, price_cents, currency, bonus_credits, is_active, sort_order, created_at, updated_at
		 FROM credit_packages WHERE id = $1 AND is_active = true`,
		packageID,
	).Scan(&p.ID, &p.Name, &p.Description, &p.Credits, &p.PriceCents,
		&p.Currency, &p.BonusCredits, &p.IsActive, &p.SortOrder, &p.CreatedAt, &p.UpdatedAt)
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
