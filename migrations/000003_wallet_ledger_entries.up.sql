BEGIN;

CREATE TABLE public.wallet_ledger_entries (
    id TEXT PRIMARY KEY CHECK (id <> '' AND id !~ '^[[:space:]]|[[:space:]]$'),
    wallet_id TEXT NOT NULL REFERENCES public.wallets(id),
    transaction_id TEXT NOT NULL REFERENCES public.wager_transactions(id),
    currency TEXT NOT NULL CHECK (currency IN ('BRL','USD')),
    direction TEXT NOT NULL CHECK (direction IN ('DEBIT','CREDIT')),
    amount_minor BIGINT NOT NULL CHECK (amount_minor > 0),
    balance_before_minor BIGINT NOT NULL CHECK (balance_before_minor >= 0),
    balance_after_minor BIGINT NOT NULL CHECK (balance_after_minor >= 0),
    created_at TIMESTAMPTZ NOT NULL CHECK (isfinite(created_at)),
    CONSTRAINT ledger_wallet_transaction_unique UNIQUE (wallet_id, transaction_id),
    -- NUMERIC is exact and avoids overflowing BIGINT while checking invalid input.
    CONSTRAINT ledger_balance_equation CHECK (
        balance_after_minor::NUMERIC = balance_before_minor::NUMERIC +
        CASE direction WHEN 'CREDIT' THEN amount_minor::NUMERIC ELSE -amount_minor::NUMERIC END
    )
);

CREATE INDEX ledger_wallet_cursor ON public.wallet_ledger_entries(wallet_id, created_at, id);

-- Validate the processed transaction and, for rollback, reverse its original entry.
CREATE FUNCTION public.validate_wallet_ledger_entry() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    tx public.wager_transactions%ROWTYPE;
    original public.wallet_ledger_entries%ROWTYPE;
BEGIN
    SELECT * INTO tx FROM public.wager_transactions WHERE id=NEW.transaction_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Ledger transaction does not exist' USING ERRCODE='23503';
    END IF;
    IF tx.status<>'PROCESSED' OR tx.kind='LOSS'
       OR ROW(NEW.wallet_id,NEW.currency,NEW.amount_minor,NEW.balance_after_minor)
          IS DISTINCT FROM ROW(tx.wallet_id,tx.currency,tx.amount_minor,tx.result_balance_minor)
       OR NEW.created_at<tx.updated_at
       OR (tx.kind='BET' AND NEW.direction<>'DEBIT')
       OR (tx.kind IN ('OPENING','WIN','REFUND') AND NEW.direction<>'CREDIT')
       OR (tx.kind='OPENING' AND NEW.balance_before_minor<>0) THEN
        RAISE EXCEPTION 'Ledger entry does not match transaction' USING ERRCODE='23514';
    END IF;
    IF tx.kind='ROLLBACK' THEN
        SELECT * INTO original FROM public.wallet_ledger_entries
        WHERE wallet_id=NEW.wallet_id AND transaction_id=tx.reference_transaction_id;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Original ledger entry is required for rollback' USING ERRCODE='23514';
        END IF;
        IF NEW.direction=original.direction OR NEW.amount_minor<>original.amount_minor THEN
            RAISE EXCEPTION 'Rollback must reverse original movement' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

-- Statement-level triggers also reject empty-table writes and TRUNCATE.
CREATE FUNCTION public.reject_wallet_ledger_mutation() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Ledger entries are append-only' USING ERRCODE='23514';
END;
$$;

CREATE TRIGGER ledger_validate BEFORE INSERT ON public.wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION public.validate_wallet_ledger_entry();
CREATE TRIGGER ledger_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON public.wallet_ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION public.reject_wallet_ledger_mutation();

COMMIT;
