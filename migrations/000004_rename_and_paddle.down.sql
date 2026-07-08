-- 000004 down
DROP INDEX IF EXISTS uq_paddle_txn;
ALTER TABLE credit_packages DROP COLUMN IF EXISTS paddle_price_id;
ALTER TABLE validations DROP COLUMN IF EXISTS name;
