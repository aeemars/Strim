-- Seed initial 100 accounts (account-001 through account-100)
-- Starting balance: 500,000,000 minor units (5,000,000 NGN)
INSERT INTO accounts (id, account_number, balance_minor, currency, created_at)
SELECT
    gen_random_uuid(),
    'account-' || to_char(s, 'FM000'),
    500000000,
    'NGN',
    now()
FROM generate_series(1, 100) AS s
ON CONFLICT (account_number) DO NOTHING;
