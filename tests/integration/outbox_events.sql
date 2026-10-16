BEGIN;
CREATE FUNCTION pg_temp.expect_error(statement TEXT, expected_state TEXT)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE actual_state TEXT;
BEGIN
 BEGIN EXECUTE statement;
 EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS actual_state = RETURNED_SQLSTATE;
  IF actual_state<>expected_state THEN RAISE EXCEPTION 'Expected %, got %: %',expected_state,actual_state,statement; END IF;
  RETURN;
 END;
 RAISE EXCEPTION 'Unexpected success: %',statement;
END; $$;

CREATE FUNCTION pg_temp.insert_event(overrides JSONB) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE s public.outbox_events%ROWTYPE;
BEGIN
 SELECT * INTO s FROM jsonb_populate_record(NULL::public.outbox_events,
 '{"event_id":"e","aggregate_id":"w","event_type":"WalletBalanceChanged",
 "correlation_id":"c","version":1,"payload":{"walletId":"w","money":{"amount":"1.00","currency":"BRL"}},
 "occurred_at":"2026-10-14T10:00:00Z","created_at":"2026-10-14T10:00:00Z",
 "attempts":0,"next_attempt_at":"2026-10-14T10:00:00Z"}'::jsonb || overrides);
 INSERT INTO public.outbox_events SELECT s.*;
END; $$;
DO $$ DECLARE patch JSONB; BEGIN
 FOR patch IN SELECT value FROM jsonb_array_elements('[
 {"event_id":" "},{"aggregate_id":""},{"correlation_id":" "},{"causation_id":""},
 {"event_type":"Unknown"},{"version":0},{"payload":[]},{"payload":"text"},
 {"occurred_at":"infinity"},{"created_at":"-infinity"},{"next_attempt_at":"infinity"},
 {"attempts":-1},{"attempts":1},{"last_error":"failed"},
 {"published_at":"2026-10-14T10:00:00Z"}
 ]'::jsonb) LOOP
  PERFORM pg_temp.expect_error(format('SELECT pg_temp.insert_event(%L::jsonb)',patch),'23514');
 END LOOP;
 PERFORM pg_temp.expect_error('SELECT pg_temp.insert_event(''{"payload":null}'')','23502');
END; $$;
SELECT pg_temp.insert_event('{}');
SELECT pg_temp.expect_error('SELECT pg_temp.insert_event(''{}'')','23505');

-- Every snapshot field, including optional causation, must survive retries unchanged.
DO $$ DECLARE assignment TEXT; BEGIN
 FOREACH assignment IN ARRAY ARRAY[
  'event_id=''changed''','aggregate_id=''changed''',
  'event_type=''WagerTransactionProcessed''','correlation_id=''changed''',
  'causation_id=''changed''','version=2','payload=''{}''::jsonb',
  'occurred_at=occurred_at+interval ''1 second''','created_at=created_at+interval ''1 second'''
 ] LOOP
  PERFORM pg_temp.expect_error('UPDATE outbox_events SET '||assignment||' WHERE event_id=''e''','23514');
 END LOOP;
END; $$;
UPDATE outbox_events SET attempts=1,last_error='temporary failure',next_attempt_at='2026-10-14T10:01:00Z' WHERE event_id='e';
SELECT pg_temp.expect_error('UPDATE outbox_events SET attempts=0','23514');
SELECT pg_temp.expect_error('UPDATE outbox_events SET published_at=''2026-10-14T10:02:00Z''','23514');
SELECT pg_temp.expect_error('UPDATE outbox_events SET published_at=''2026-10-13T10:00:00Z'',last_error=NULL','23514');
SELECT pg_temp.expect_error('DELETE FROM outbox_events','23514');
SELECT pg_temp.expect_error('TRUNCATE outbox_events','23514');

-- Rolling back an unconfirmed publication leaves the same event available again.
SAVEPOINT before_publication;
UPDATE outbox_events SET attempts=2,last_error=NULL,published_at='2026-10-14T10:02:00Z' WHERE event_id='e';
ROLLBACK TO SAVEPOINT before_publication;
DO $$ BEGIN
 IF NOT EXISTS (SELECT 1 FROM outbox_events WHERE event_id='e' AND attempts=1 AND published_at IS NULL
 AND payload->'money'->>'amount'='1.00') THEN RAISE EXCEPTION 'Snapshot or retry state lost'; END IF;
END; $$;
UPDATE outbox_events SET attempts=2,last_error=NULL,published_at='2026-10-14T10:02:00Z' WHERE event_id='e';
SELECT pg_temp.expect_error('UPDATE outbox_events SET published_at=NULL','23514');
SELECT pg_temp.expect_error('UPDATE outbox_events SET attempts=3','23514');
SELECT pg_temp.insert_event('{"event_id":"later","next_attempt_at":"2026-10-15T10:00:00Z"}');
SELECT pg_temp.insert_event('{"event_id":"due","event_type":"WagerTransactionProcessed"}');
DO $$ DECLARE selected TEXT; BEGIN
 SELECT event_id INTO selected FROM outbox_events
 WHERE published_at IS NULL AND next_attempt_at<='2026-10-14T10:03:00Z'
 ORDER BY next_attempt_at,event_id LIMIT 1 FOR UPDATE SKIP LOCKED;
 IF selected IS DISTINCT FROM 'due' THEN RAISE EXCEPTION 'Wrong due event'; END IF;
END; $$;
ROLLBACK;
