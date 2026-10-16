BEGIN;

-- message_id is the application envelope identity, not the SQS receipt handle.
CREATE TABLE public.inbox_messages (
    consumer_name TEXT NOT NULL CHECK (consumer_name<>'' AND consumer_name !~ '^[[:space:]]|[[:space:]]$'),
    message_id TEXT NOT NULL CHECK (message_id<>'' AND message_id !~ '^[[:space:]]|[[:space:]]$'),
    payload_hash TEXT NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP CHECK (isfinite(received_at)),
    completed_at TIMESTAMPTZ CHECK (isfinite(completed_at)),
    PRIMARY KEY (consumer_name,message_id),
    CONSTRAINT inbox_completion_order CHECK (completed_at IS NULL OR completed_at>=received_at)
);

-- Completion means durable handling, including a persisted pending reference.
-- The consumer must commit this row together with domain changes and outbox events.
CREATE FUNCTION public.protect_inbox_message() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.completed_at IS NOT NULL THEN
        RAISE EXCEPTION 'Completed inbox message cannot change' USING ERRCODE='23514';
    END IF;
    IF ROW(NEW.consumer_name,NEW.message_id,NEW.payload_hash,NEW.received_at)
       IS DISTINCT FROM ROW(OLD.consumer_name,OLD.message_id,OLD.payload_hash,OLD.received_at) THEN
        RAISE EXCEPTION 'Inbox identity and hash are immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION public.reject_inbox_removal() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Inbox messages cannot be removed' USING ERRCODE='23514';
END;
$$;
CREATE TRIGGER inbox_protect BEFORE UPDATE ON public.inbox_messages
    FOR EACH ROW EXECUTE FUNCTION public.protect_inbox_message();
CREATE TRIGGER inbox_keep BEFORE DELETE OR TRUNCATE ON public.inbox_messages
    FOR EACH STATEMENT EXECUTE FUNCTION public.reject_inbox_removal();

COMMENT ON COLUMN public.inbox_messages.payload_hash IS 'SHA-256 of the incoming message content; the consumer must compare it on every duplicate identity.';
COMMIT;
