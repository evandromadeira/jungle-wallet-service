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

CREATE FUNCTION pg_temp.insert_message(overrides JSONB) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE s public.inbox_messages%ROWTYPE;
BEGIN
 SELECT * INTO s FROM jsonb_populate_record(NULL::public.inbox_messages,
 '{"consumer_name":"wagers","message_id":"msg-1","received_at":"2026-10-14T10:00:00Z"}'::jsonb
 || jsonb_build_object('payload_hash',repeat('a',64)) || overrides);
 INSERT INTO public.inbox_messages SELECT s.*;
END; $$;
DO $$ DECLARE patch JSONB; BEGIN
 FOR patch IN SELECT value FROM jsonb_array_elements('[
 {"consumer_name":""},{"consumer_name":" wagers"},{"message_id":""},{"message_id":"msg "},
 {"payload_hash":"abc"},{"payload_hash":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
 {"received_at":"infinity"},{"completed_at":"infinity"},
 {"completed_at":"2026-10-14T09:59:59Z"}
 ]'::jsonb) LOOP
  PERFORM pg_temp.expect_error(format('SELECT pg_temp.insert_message(%L::jsonb)',patch),'23514');
 END LOOP;
 FOREACH patch IN ARRAY ARRAY['{"consumer_name":null}'::jsonb,'{"message_id":null}'::jsonb,'{"payload_hash":null}'::jsonb,'{"received_at":null}'::jsonb] LOOP
  PERFORM pg_temp.expect_error(format('SELECT pg_temp.insert_message(%L::jsonb)',patch),'23502');
 END LOOP;
END; $$;
SELECT pg_temp.insert_message('{}');
SELECT pg_temp.expect_error('SELECT pg_temp.insert_message(''{}'')','23505');
SELECT pg_temp.expect_error('SELECT pg_temp.insert_message(jsonb_build_object(''payload_hash'',repeat(''b'',64)))','23505');
-- Consumer-scoped identity: another consumer may handle the same message.
SELECT pg_temp.insert_message('{"consumer_name":"audit"}');
SELECT pg_temp.insert_message('{"message_id":"msg-2"}');
DO $$ DECLARE assignment TEXT; BEGIN
 FOREACH assignment IN ARRAY ARRAY[
  'consumer_name=''other''','message_id=''other''',
  'payload_hash=repeat(''b'',64)','received_at=received_at+interval ''1 second'''
 ] LOOP
  PERFORM pg_temp.expect_error('UPDATE inbox_messages SET '||assignment||' WHERE consumer_name=''wagers'' AND message_id=''msg-1''','23514');
 END LOOP;
END; $$;
SELECT pg_temp.expect_error('DELETE FROM inbox_messages','23514');
SELECT pg_temp.expect_error('TRUNCATE inbox_messages','23514');
SAVEPOINT before_completion;
UPDATE inbox_messages SET completed_at='2026-10-14T10:01:00Z' WHERE consumer_name='wagers' AND message_id='msg-1';
ROLLBACK TO SAVEPOINT before_completion;
DO $$ BEGIN
 IF NOT EXISTS (SELECT 1 FROM inbox_messages WHERE consumer_name='wagers' AND message_id='msg-1'
 AND completed_at IS NULL AND payload_hash=repeat('a',64)) THEN RAISE EXCEPTION 'Completion rollback lost original state'; END IF;
END; $$;
UPDATE inbox_messages SET completed_at='2026-10-14T10:01:00Z' WHERE consumer_name='wagers' AND message_id='msg-1';
SELECT pg_temp.expect_error('UPDATE inbox_messages SET completed_at=NULL WHERE consumer_name=''wagers'' AND message_id=''msg-1''','23514');
SELECT pg_temp.expect_error('UPDATE inbox_messages SET completed_at=completed_at WHERE consumer_name=''wagers'' AND message_id=''msg-1''','23514');
-- A failed SQL transaction must not leave a new inbox identity behind.
SAVEPOINT before_receipt;
SELECT pg_temp.insert_message('{"message_id":"rolled-back"}');
ROLLBACK TO SAVEPOINT before_receipt;
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM inbox_messages WHERE message_id='rolled-back') OR
 (SELECT count(*) FROM inbox_messages)<>3 THEN RAISE EXCEPTION 'Unexpected inbox rows'; END IF;
END; $$;
ROLLBACK;
