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

INSERT INTO wallets(id,player_id,currency) VALUES ('w','p','BRL'),('u','p','USD');

-- Build rows through the real table type, so mutations exercise actual constraints.
CREATE FUNCTION pg_temp.insert_wager(overrides JSONB) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE s public.wager_transactions%ROWTYPE;
BEGIN
 SELECT * INTO s FROM jsonb_populate_record(NULL::public.wager_transactions,
 '{"id":"t","origin":"EXTERNAL","wallet_id":"w","player_id":"p","currency":"BRL",
 "kind":"BET","amount_minor":100,"provider_id":"a","external_transaction_id":"ext",
 "idempotency_key":"key","round_id":"r","game_id":"g","status":"PENDING","attempts":0,
 "created_at":"2026-10-14T10:00:00Z","updated_at":"2026-10-14T10:00:00Z"}'::jsonb
 || jsonb_build_object('payload_hash',repeat('a',64)) || overrides);
 INSERT INTO public.wager_transactions SELECT s.*;
END; $$;

DO $$
DECLARE patch JSONB;
BEGIN
 FOR patch IN SELECT value FROM jsonb_array_elements('[
 {"origin":"BAD"},{"kind":"OPENING"},{"provider_id":null},{"provider_id":" "},
 {"external_transaction_id":null},{"idempotency_key":null},{"payload_hash":null},
 {"payload_hash":"x"},{"round_id":null},{"game_id":null},
 {"amount_minor":-1},{"amount_minor":0},{"kind":"LOSS","amount_minor":1},
 {"kind":"REFUND"},{"kind":"ROLLBACK"},{"reference_external_transaction_id":"ext"},
 {"status":"BAD"},{"status":"PROCESSED"},{"status":"REJECTED"},{"status":"FAILED"},
 {"status":"PENDING_REFERENCE"},{"result_balance_minor":1},
 {"attempts":-1},{"updated_at":"2020-01-01"},{"updated_at":"infinity"},
 {"reference_external_transaction_id":"other"}
 ]'::jsonb) LOOP
  PERFORM pg_temp.expect_error(format('SELECT pg_temp.insert_wager(%L::jsonb)',patch),'23514');
 END LOOP;
END; $$;
SELECT pg_temp.expect_error('SELECT pg_temp.insert_wager(''{"wallet_id":"missing"}'')','23503');
SELECT pg_temp.expect_error('SELECT pg_temp.insert_wager(''{"player_id":"other"}'')','23503');
SELECT pg_temp.expect_error('SELECT pg_temp.insert_wager(''{"currency":"USD"}'')','23503');
SELECT pg_temp.insert_wager('{}');
SELECT pg_temp.expect_error('SELECT pg_temp.insert_wager(''{"id":"duplicate"}'')','23505');
SELECT pg_temp.expect_error('SELECT pg_temp.insert_wager(''{"id":"duplicate","external_transaction_id":"different"}'')','23505');
-- Separate providers may reuse external IDs and keys.
SELECT pg_temp.insert_wager('{"id":"provider-b","provider_id":"b"}');
SELECT pg_temp.expect_error('UPDATE wager_transactions SET amount_minor=200 WHERE id=''t''','23514');
SELECT pg_temp.expect_error('UPDATE wager_transactions SET updated_at=''2020-01-01'' WHERE id=''t''','23514');
UPDATE wager_transactions SET status='PROCESSED',result_balance_minor=900 WHERE id='t';
SELECT pg_temp.expect_error('UPDATE wager_transactions SET result_balance_minor=800 WHERE id=''t''','23514');
SELECT pg_temp.expect_error('UPDATE wager_transactions SET status=''PENDING'',result_balance_minor=NULL WHERE id=''t''','23514');
SELECT pg_temp.expect_error('DELETE FROM wager_transactions WHERE id=''t''','23514');

SELECT pg_temp.insert_wager('{"id":"refund","external_transaction_id":"refund","idempotency_key":"refund",
 "kind":"REFUND","reference_external_transaction_id":"ext","status":"PENDING_REFERENCE"}');
SELECT pg_temp.expect_error('UPDATE wager_transactions SET reference_transaction_id=''provider-b'',status=''PENDING'' WHERE id=''refund''','23514');
UPDATE wager_transactions SET reference_transaction_id='t',status='PENDING' WHERE id='refund';
UPDATE wager_transactions SET status='PROCESSED',result_balance_minor=1000 WHERE id='refund';
SELECT pg_temp.expect_error('SELECT pg_temp.insert_wager(''{"id":"rollback","external_transaction_id":"rollback","idempotency_key":"rollback","kind":"ROLLBACK","reference_external_transaction_id":"ext","reference_transaction_id":"t","status":"PROCESSED","result_balance_minor":1000}'')','23505');
SELECT pg_temp.expect_error('SELECT pg_temp.insert_wager(''{"id":"wrong","external_transaction_id":"wrong","idempotency_key":"wrong","kind":"REFUND","amount_minor":99,"reference_external_transaction_id":"ext","reference_transaction_id":"t"}'')','23514');

SELECT pg_temp.insert_wager('{"id":"loss","external_transaction_id":"loss","idempotency_key":"loss","kind":"LOSS","amount_minor":0,"status":"PROCESSED","result_balance_minor":1000}');
SELECT pg_temp.insert_wager('{"id":"rejected","external_transaction_id":"rejected","idempotency_key":"rejected","status":"REJECTED","failure_code":"INSUFFICIENT_FUNDS"}');
SELECT pg_temp.insert_wager('{"id":"failed","external_transaction_id":"failed","idempotency_key":"failed","status":"FAILED","failure_code":"INFRASTRUCTURE_FAILURE"}');
SELECT pg_temp.expect_error('UPDATE wager_transactions SET failure_code=''BALANCE_OVERFLOW'' WHERE id=''rejected''','23514');
SELECT pg_temp.expect_error('UPDATE wager_transactions SET status=''PENDING'',failure_code=NULL WHERE id=''failed''','23514');
INSERT INTO wager_transactions(id,origin,wallet_id,player_id,currency,kind,amount_minor,status,result_balance_minor)
VALUES ('opening','INTERNAL','w','p','BRL','OPENING',1000,'PROCESSED',1000);
SELECT pg_temp.expect_error('INSERT INTO wager_transactions(id,origin,wallet_id,player_id,currency,kind,amount_minor) VALUES(''opening2'',''INTERNAL'',''w'',''p'',''BRL'',''OPENING'',1000)','23505');
SELECT pg_temp.expect_error('INSERT INTO wager_transactions(id,origin,wallet_id,player_id,currency,kind,amount_minor,status,result_balance_minor) VALUES(''opening3'',''INTERNAL'',''u'',''p'',''USD'',''OPENING'',1000,''PROCESSED'',900)','23514');
DO $$ BEGIN
 IF (SELECT result_balance_minor FROM wager_transactions WHERE id='t')<>900 THEN RAISE EXCEPTION 'Replay result changed'; END IF;
END; $$;
ROLLBACK;
