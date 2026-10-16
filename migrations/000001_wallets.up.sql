BEGIN;

CREATE TABLE public.wallets (
    id TEXT PRIMARY KEY,
    player_id TEXT NOT NULL,
    currency TEXT NOT NULL,
    balance_minor BIGINT NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT wallets_id_valid CHECK (id <> '' AND id !~ '^[[:space:]]|[[:space:]]$'),
    CONSTRAINT wallets_player_valid CHECK (player_id <> '' AND player_id !~ '^[[:space:]]|[[:space:]]$'),
    CONSTRAINT wallets_currency_valid CHECK (currency IN ('BRL', 'USD')),
    CONSTRAINT wallets_balance_nonnegative CHECK (balance_minor >= 0),
    CONSTRAINT wallets_version_positive CHECK (version >= 1),
    CONSTRAINT wallets_dates_valid CHECK (
        isfinite(created_at) AND isfinite(updated_at) AND updated_at >= created_at
    ),
    CONSTRAINT wallets_player_currency_unique UNIQUE (player_id, currency)
);

COMMENT ON COLUMN public.wallets.balance_minor IS 'Exact signed int64 storage in minor units; wallet balances cannot be negative.';
COMMENT ON COLUMN public.wallets.version IS 'Starts at 1; the transactional repository must increment only when balance changes.';

COMMIT;
