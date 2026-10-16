BEGIN;

-- Envelope metadata and data snapshot are immutable; delivery metadata may advance.
CREATE TABLE public.outbox_events (
    event_id TEXT PRIMARY KEY CHECK (event_id<>'' AND event_id !~ '^[[:space:]]|[[:space:]]$'),
    aggregate_id TEXT NOT NULL CHECK (aggregate_id<>'' AND aggregate_id !~ '^[[:space:]]|[[:space:]]$'),
    event_type TEXT NOT NULL CHECK (event_type IN (
        'WagerTransactionProcessed','WagerTransactionRejected',
        'WalletBalanceChanged','WagerTransactionPendingReference')),
    correlation_id TEXT NOT NULL CHECK (correlation_id<>'' AND correlation_id !~ '^[[:space:]]|[[:space:]]$'),
    causation_id TEXT CHECK (causation_id<>'' AND causation_id !~ '^[[:space:]]|[[:space:]]$'),
    version INTEGER NOT NULL CHECK (version>0),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload)='object'),
    occurred_at TIMESTAMPTZ NOT NULL CHECK (isfinite(occurred_at)),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP CHECK (isfinite(created_at)),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts>=0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP CHECK (isfinite(next_attempt_at)),
    last_error TEXT CHECK (last_error<>''),
    published_at TIMESTAMPTZ CHECK (isfinite(published_at)),
    CONSTRAINT outbox_publication_valid CHECK (
        published_at IS NULL OR (published_at>=created_at AND attempts>0 AND last_error IS NULL)
    )
);

-- Publishers will lock due rows with FOR UPDATE SKIP LOCKED within a transaction.
CREATE INDEX outbox_due ON public.outbox_events(next_attempt_at,event_id)
    WHERE published_at IS NULL;

CREATE FUNCTION public.protect_outbox_event() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        IF NEW.attempts<>0 OR NEW.published_at IS NOT NULL OR NEW.last_error IS NOT NULL THEN
            RAISE EXCEPTION 'New outbox event must be unpublished and unattempted' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.published_at IS NOT NULL THEN
        RAISE EXCEPTION 'Published outbox event cannot change' USING ERRCODE='23514';
    END IF;
    IF ROW(NEW.event_id,NEW.aggregate_id,NEW.event_type,NEW.correlation_id,NEW.causation_id,
           NEW.version,NEW.payload,NEW.occurred_at,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.event_id,OLD.aggregate_id,OLD.event_type,OLD.correlation_id,OLD.causation_id,
           OLD.version,OLD.payload,OLD.occurred_at,OLD.created_at)
       OR NEW.attempts<OLD.attempts THEN
        RAISE EXCEPTION 'Outbox snapshot is immutable and attempts cannot decrease' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;

-- Retention is deliberately not implemented; migrations use explicit DROP instead.
CREATE FUNCTION public.reject_outbox_removal() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Outbox events cannot be removed' USING ERRCODE='23514';
END;
$$;
CREATE TRIGGER outbox_protect BEFORE INSERT OR UPDATE ON public.outbox_events
    FOR EACH ROW EXECUTE FUNCTION public.protect_outbox_event();
CREATE TRIGGER outbox_keep BEFORE DELETE OR TRUNCATE ON public.outbox_events
    FOR EACH STATEMENT EXECUTE FUNCTION public.reject_outbox_removal();

COMMENT ON COLUMN public.outbox_events.payload IS 'Immutable event data object. Envelope fields are stored separately; typed Go constructors validate event-specific data.';
COMMENT ON COLUMN public.outbox_events.attempts IS 'Committed delivery attempts. An interrupted uncommitted attempt may not be counted.';
COMMIT;
