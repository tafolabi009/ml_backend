-- 000004: validation rename + paddle billing support
ALTER TABLE validations ADD COLUMN IF NOT EXISTS name VARCHAR(120);
ALTER TABLE credit_packages ADD COLUMN IF NOT EXISTS paddle_price_id VARCHAR(255);
CREATE UNIQUE INDEX IF NOT EXISTS uq_paddle_txn
    ON credit_transactions(reference_id)
    WHERE reference_type = 'paddle' AND reference_id IS NOT NULL;
