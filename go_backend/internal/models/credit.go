package models

import "time"

// CreditBalance represents a user's credit balance
type CreditBalance struct {
	ID                string    `json:"id" db:"id"`
	UserID            string    `json:"user_id" db:"user_id"`
	Balance           int64     `json:"balance" db:"balance"`
	LifetimePurchased int64     `json:"lifetime_purchased" db:"lifetime_purchased"`
	LifetimeUsed      int64     `json:"lifetime_used" db:"lifetime_used"`
	CreatedAt         time.Time `json:"created_at" db:"created_at"`
	UpdatedAt         time.Time `json:"updated_at" db:"updated_at"`
}

// CreditTransaction represents a credit transaction (purchase, deduction, refund, etc.)
type CreditTransaction struct {
	ID            string                 `json:"id" db:"id"`
	UserID        string                 `json:"user_id" db:"user_id"`
	Type          string                 `json:"type" db:"type"` // purchase, deduction, refund, bonus, expiry
	Amount        int64                  `json:"amount" db:"amount"`
	BalanceAfter  int64                  `json:"balance_after" db:"balance_after"`
	Description   string                 `json:"description,omitempty" db:"description"`
	ReferenceType *string                `json:"reference_type,omitempty" db:"reference_type"`
	ReferenceID   *string                `json:"reference_id,omitempty" db:"reference_id"`
	Metadata      map[string]interface{} `json:"metadata,omitempty" db:"metadata"`
	CreatedAt     time.Time              `json:"created_at" db:"created_at"`
	ReceiptURL    *string                `json:"receipt_url,omitempty" db:"-"`
}

// CreditPackage represents a purchasable credit package
type CreditPackage struct {
	ID            string                 `json:"id" db:"id"`
	Name          string                 `json:"name" db:"name"`
	Description   string                 `json:"description,omitempty" db:"description"`
	Credits       int64                  `json:"credits" db:"credits"`
	PriceCents    int64                  `json:"price_cents" db:"price_cents"`
	Currency      string                 `json:"currency" db:"currency"`
	BonusCredits  int64                  `json:"bonus_credits" db:"bonus_credits"`
	PaddlePriceID *string                `json:"paddle_price_id,omitempty" db:"paddle_price_id"`
	IsActive      bool                   `json:"is_active" db:"is_active"`
	SortOrder     int                    `json:"sort_order" db:"sort_order"`
	Metadata      map[string]interface{} `json:"metadata,omitempty" db:"metadata"`
	CreatedAt     time.Time              `json:"created_at" db:"created_at"`
	UpdatedAt     time.Time              `json:"updated_at" db:"updated_at"`
}

// CreditCost defines how many credits an operation costs
type CreditCost struct {
	ID              string    `json:"id" db:"id"`
	Operation       string    `json:"operation" db:"operation"`
	CreditsRequired int64     `json:"credits_required" db:"credits_required"`
	Description     string    `json:"description,omitempty" db:"description"`
	IsActive        bool      `json:"is_active" db:"is_active"`
	CreatedAt       time.Time `json:"created_at" db:"created_at"`
	UpdatedAt       time.Time `json:"updated_at" db:"updated_at"`
}

// PurchaseCreditsRequest is the request to purchase credits
type PurchaseCreditsRequest struct {
	PackageID     string `json:"package_id" binding:"required"`
	PaymentMethod string `json:"payment_method,omitempty"` // for future payment integration
}

// PurchaseCreditsResponse is the response after purchasing credits
type PurchaseCreditsResponse struct {
	TransactionID string        `json:"transaction_id"`
	PackageName   string        `json:"package_name"`
	CreditsAdded  int64         `json:"credits_added"`
	BonusCredits  int64         `json:"bonus_credits"`
	TotalAdded    int64         `json:"total_added"`
	NewBalance    int64         `json:"new_balance"`
	AmountCharged int64         `json:"amount_charged_cents"`
	Currency      string        `json:"currency"`
	Balance       CreditBalance `json:"balance"`
}

// CreditBalanceResponse is the API response for credit balance
type CreditBalanceResponse struct {
	Balance           int64        `json:"balance"`
	LifetimePurchased int64        `json:"lifetime_purchased"`
	LifetimeUsed      int64        `json:"lifetime_used"`
	CreditCosts       []CreditCost `json:"credit_costs"`
}

// CreditHistoryResponse is the API response for credit transaction history
type CreditHistoryResponse struct {
	Transactions []CreditTransaction `json:"transactions"`
	Pagination   Pagination          `json:"pagination"`
}

// CreditPackagesResponse is the API response listing available packages
type CreditPackagesResponse struct {
	Packages []CreditPackage `json:"packages"`
}
