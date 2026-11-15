CREATE TABLE IF NOT EXISTS accounts (
    id UUID PRIMARY KEY,
    account_number TEXT NOT NULL UNIQUE,
    balance_minor BIGINT NOT NULL DEFAULT 0 CHECK (balance_minor >= 0),
    currency CHAR(3) NOT NULL DEFAULT 'NGN' CHECK (currency = upper(currency)),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS processed_transactions (
    idempotency_key TEXT PRIMARY KEY,
    transaction_id UUID NOT NULL UNIQUE,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ledger_entries (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id UUID NOT NULL REFERENCES processed_transactions(transaction_id),
    account_id UUID NOT NULL REFERENCES accounts(id),
    entry_type TEXT NOT NULL CHECK (entry_type IN ('DEBIT', 'CREDIT')),
    amount_minor BIGINT NOT NULL CHECK (amount_minor > 0),
    currency CHAR(3) NOT NULL CHECK (currency = upper(currency)),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ledger_entries_transaction_id_idx
    ON ledger_entries (transaction_id);

CREATE INDEX IF NOT EXISTS ledger_entries_account_created_at_idx
    ON ledger_entries (account_id, created_at);

CREATE OR REPLACE FUNCTION prevent_unbalanced_transaction()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM ledger_entries entry
        WHERE entry.transaction_id = NEW.transaction_id
        GROUP BY entry.transaction_id
        HAVING COUNT(*) > 2
            OR COUNT(*) FILTER (WHERE entry.entry_type = 'DEBIT') > 1
            OR COUNT(*) FILTER (WHERE entry.entry_type = 'CREDIT') > 1
    ) THEN
        RAISE EXCEPTION 'transaction % has more than one debit or credit entry', NEW.transaction_id;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS ledger_entries_shape_trigger ON ledger_entries;
CREATE CONSTRAINT TRIGGER ledger_entries_shape_trigger
AFTER INSERT OR UPDATE ON ledger_entries
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION prevent_unbalanced_transaction();
