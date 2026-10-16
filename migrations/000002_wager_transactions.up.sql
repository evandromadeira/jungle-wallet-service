BEGIN;

ALTER TABLE public.wallets ADD CONSTRAINT wallets_identity_currency_unique UNIQUE (id, player_id, currency);

CREATE TABLE public.wager_transactions (
 id TEXT PRIMARY KEY CHECK (id <> '' AND id !~ '^[[:space:]]|[[:space:]]$'),
 origin TEXT NOT NULL CHECK (origin IN ('INTERNAL','EXTERNAL')),
 wallet_id TEXT NOT NULL,
 player_id TEXT NOT NULL,
 currency TEXT NOT NULL,
 kind TEXT NOT NULL CHECK (kind IN ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
 amount_minor BIGINT NOT NULL,
 provider_id TEXT,
 external_transaction_id TEXT,
 idempotency_key TEXT,
 payload_hash TEXT,
 round_id TEXT,
 game_id TEXT,
 reference_external_transaction_id TEXT,
 reference_transaction_id TEXT REFERENCES public.wager_transactions(id),
 status TEXT NOT NULL DEFAULT 'PENDING'
   CHECK (status IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
 failure_code TEXT,
 result_balance_minor BIGINT CHECK (result_balance_minor >= 0),
 attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
 next_attempt_at TIMESTAMPTZ CHECK (isfinite(next_attempt_at)),
 created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
 updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
 CONSTRAINT wagers_wallet_fk FOREIGN KEY(wallet_id,player_id,currency)
   REFERENCES public.wallets(id,player_id,currency),
 CONSTRAINT wagers_dates CHECK (isfinite(created_at) AND isfinite(updated_at) AND updated_at >= created_at),
 CONSTRAINT wagers_amount CHECK ((kind='LOSS' AND amount_minor=0) OR (kind<>'LOSS' AND amount_minor>0)),
 CONSTRAINT wagers_origin CHECK ((
   (origin='INTERNAL' AND kind='OPENING' AND provider_id IS NULL AND external_transaction_id IS NULL
    AND idempotency_key IS NULL AND payload_hash IS NULL AND round_id IS NULL AND game_id IS NULL
    AND reference_external_transaction_id IS NULL AND reference_transaction_id IS NULL)
   OR
   (origin='EXTERNAL' AND kind<>'OPENING'
    AND provider_id IS NOT NULL AND provider_id<>'' AND provider_id !~ '^[[:space:]]|[[:space:]]$'
    AND external_transaction_id IS NOT NULL AND external_transaction_id<>'' AND external_transaction_id !~ '^[[:space:]]|[[:space:]]$'
    AND idempotency_key IS NOT NULL AND idempotency_key<>'' AND idempotency_key !~ '^[[:space:]]|[[:space:]]$'
    AND payload_hash IS NOT NULL AND payload_hash ~ '^[0-9a-f]{64}$'
    AND round_id IS NOT NULL AND round_id<>'' AND round_id !~ '^[[:space:]]|[[:space:]]$'
    AND game_id IS NOT NULL AND game_id<>'' AND game_id !~ '^[[:space:]]|[[:space:]]$')
 ) IS TRUE),
 CONSTRAINT wagers_reference CHECK (
   (kind NOT IN ('REFUND','ROLLBACK') OR reference_external_transaction_id IS NOT NULL)
   AND (reference_external_transaction_id IS NULL OR (
     kind IN ('WIN','REFUND','ROLLBACK') AND reference_external_transaction_id<>''
     AND reference_external_transaction_id !~ '^[[:space:]]|[[:space:]]$'
     AND reference_external_transaction_id<>external_transaction_id))
   AND (reference_transaction_id IS NULL OR (reference_external_transaction_id IS NOT NULL AND reference_transaction_id<>id))
 ),
 CONSTRAINT wagers_outcome CHECK ((
   (status IN ('PENDING','PENDING_REFERENCE') AND failure_code IS NULL AND result_balance_minor IS NULL)
   OR (status='PROCESSED' AND failure_code IS NULL AND result_balance_minor IS NOT NULL)
   OR (status='REJECTED' AND failure_code IN (
     'INSUFFICIENT_FUNDS','REVERSAL_INSUFFICIENT_FUNDS','REFERENCE_NOT_FOUND',
     'REFERENCE_UNSUCCESSFUL','REFERENCE_MISMATCH','ALREADY_REVERSED','BALANCE_OVERFLOW'))
   OR (status='FAILED' AND failure_code='INFRASTRUCTURE_FAILURE')
 ) IS TRUE),
 CONSTRAINT wagers_waiting CHECK (status<>'PENDING_REFERENCE' OR
   (reference_external_transaction_id IS NOT NULL AND reference_transaction_id IS NULL)),
 CONSTRAINT wagers_processed_reference CHECK (status<>'PROCESSED' OR
   reference_external_transaction_id IS NULL OR reference_transaction_id IS NOT NULL),
 CONSTRAINT wagers_opening_result CHECK (kind<>'OPENING' OR status<>'PROCESSED' OR result_balance_minor=amount_minor),
 CONSTRAINT wagers_provider_external_unique UNIQUE(provider_id,external_transaction_id),
 CONSTRAINT wagers_provider_key_unique UNIQUE(provider_id,idempotency_key)
);

CREATE UNIQUE INDEX wagers_opening_unique ON public.wager_transactions(wallet_id) WHERE kind='OPENING';
-- Conservative policy: a processed target can be reversed successfully only once,
-- by either REFUND or ROLLBACK. Rolling back a REFUND does not reopen its original BET.
CREATE UNIQUE INDEX wagers_successful_reversal_unique ON public.wager_transactions(reference_transaction_id)
 WHERE status='PROCESSED' AND kind IN ('REFUND','ROLLBACK');
CREATE INDEX wagers_pending_retry ON public.wager_transactions(next_attempt_at,id)
 WHERE status IN ('PENDING','PENDING_REFERENCE');

CREATE FUNCTION public.protect_wager_transaction() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE original public.wager_transactions%ROWTYPE;
BEGIN
 IF TG_OP='DELETE' THEN
  RAISE EXCEPTION 'Wager transactions cannot be deleted' USING ERRCODE='23514';
 END IF;
 IF TG_OP='UPDATE' THEN
  IF OLD.status IN ('PROCESSED','REJECTED','FAILED') THEN
   RAISE EXCEPTION 'Terminal wager cannot change' USING ERRCODE='23514';
  END IF;
  IF ROW(NEW.id,NEW.origin,NEW.wallet_id,NEW.player_id,NEW.currency,NEW.kind,NEW.amount_minor,
    NEW.provider_id,NEW.external_transaction_id,NEW.idempotency_key,NEW.payload_hash,
    NEW.round_id,NEW.game_id,NEW.reference_external_transaction_id,NEW.created_at)
   IS DISTINCT FROM ROW(OLD.id,OLD.origin,OLD.wallet_id,OLD.player_id,OLD.currency,OLD.kind,OLD.amount_minor,
    OLD.provider_id,OLD.external_transaction_id,OLD.idempotency_key,OLD.payload_hash,
    OLD.round_id,OLD.game_id,OLD.reference_external_transaction_id,OLD.created_at)
   OR NEW.updated_at<OLD.updated_at OR NEW.attempts<OLD.attempts
   OR (OLD.reference_transaction_id IS NOT NULL AND NEW.reference_transaction_id IS DISTINCT FROM OLD.reference_transaction_id)
  THEN
   RAISE EXCEPTION 'Immutable wager data or progression changed' USING ERRCODE='23514';
  END IF;
 END IF;
 IF NEW.reference_transaction_id IS NOT NULL THEN
  SELECT * INTO original FROM public.wager_transactions WHERE id=NEW.reference_transaction_id;
  IF NOT FOUND THEN
   RAISE EXCEPTION 'Reference does not exist' USING ERRCODE='23503';
  END IF;
  IF original.status<>'PROCESSED' OR original.origin<>'EXTERNAL'
   OR ROW(NEW.provider_id,NEW.player_id,NEW.wallet_id,NEW.currency,NEW.round_id,NEW.reference_external_transaction_id)
      IS DISTINCT FROM ROW(original.provider_id,original.player_id,original.wallet_id,original.currency,original.round_id,original.external_transaction_id)
   OR NOT ((NEW.kind IN ('WIN','REFUND') AND original.kind='BET')
        OR (NEW.kind='ROLLBACK' AND original.kind IN ('BET','WIN','REFUND')))
   OR (NEW.kind IN ('REFUND','ROLLBACK') AND NEW.amount_minor<>original.amount_minor)
  THEN
   RAISE EXCEPTION 'Reference is incompatible or not processed' USING ERRCODE='23514';
  END IF;
 END IF;
 RETURN NEW;
END;
$$;

CREATE TRIGGER wagers_protect BEFORE INSERT OR UPDATE OR DELETE ON public.wager_transactions
 FOR EACH ROW EXECUTE FUNCTION public.protect_wager_transaction();

COMMIT;
