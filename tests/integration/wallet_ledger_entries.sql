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

-- A processed bet with a 100-cent debit and a 900-cent resulting balance.
SELECT pg_temp.insert_wager('{"status":"PROCESSED","result_balance_minor":900}');
CREATE FUNCTION pg_temp.insert_entry(overrides JSONB) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE s public.wallet_ledger_entries%ROWTYPE;
BEGIN
 SELECT * INTO s FROM jsonb_populate_record(NULL::public.wallet_ledger_entries,
 '{"id":"entry","wallet_id":"w","transaction_id":"t","currency":"BRL",
 "direction":"DEBIT","amount_minor":100,"balance_before_minor":1000,
 "balance_after_minor":900,"created_at":"2026-10-14T10:00:00Z"}'::jsonb || overrides);
 INSERT INTO public.wallet_ledger_entries SELECT s.*;
END; $$;

DO $$ DECLARE patch JSONB; BEGIN
 FOR patch IN SELECT value FROM jsonb_array_elements('[
 {"id":" "},{"wallet_id":"u"},{"currency":"USD"},{"direction":"OTHER"},
 {"direction":"CREDIT","balance_before_minor":800},
 {"amount_minor":0},{"amount_minor":-1},{"amount_minor":101},
 {"balance_before_minor":-1},{"balance_after_minor":-1},
 {"balance_before_minor":1001},{"balance_after_minor":901},
 {"created_at":"infinity"},{"created_at":"2026-10-13T10:00:00Z"},
 {"balance_before_minor":9223372036854775807}
 ]'::jsonb) LOOP
  PERFORM pg_temp.expect_error(format('SELECT pg_temp.insert_entry(%L::jsonb)',patch),'23514');
 END LOOP;
 PERFORM pg_temp.expect_error('SELECT pg_temp.insert_entry(''{"transaction_id":"missing"}'')','23503');
 PERFORM pg_temp.expect_error('SELECT pg_temp.insert_entry(''{"id":null}'')','23502');
END; $$;
SELECT pg_temp.insert_entry('{}');
SELECT pg_temp.expect_error('SELECT pg_temp.insert_entry(''{"id":"duplicate"}'')','23505');
SELECT pg_temp.expect_error('UPDATE wallet_ledger_entries SET amount_minor=amount_minor','23514');
SELECT pg_temp.expect_error('DELETE FROM wallet_ledger_entries','23514');
SELECT pg_temp.expect_error('TRUNCATE wallet_ledger_entries','23514');

-- Pending, rejected and LOSS operations cannot create ledger entries.
SELECT pg_temp.insert_wager('{"id":"pending","external_transaction_id":"pending","idempotency_key":"pending"}');
SELECT pg_temp.insert_wager('{"id":"rejected","external_transaction_id":"rejected","idempotency_key":"rejected","status":"REJECTED","failure_code":"INSUFFICIENT_FUNDS"}');
SELECT pg_temp.insert_wager('{"id":"loss","external_transaction_id":"loss","idempotency_key":"loss","kind":"LOSS","amount_minor":0,"status":"PROCESSED","result_balance_minor":900}');
SELECT pg_temp.expect_error('SELECT pg_temp.insert_entry(''{"id":"p","transaction_id":"pending"}'')','23514');
SELECT pg_temp.expect_error('SELECT pg_temp.insert_entry(''{"id":"r","transaction_id":"rejected"}'')','23514');
SELECT pg_temp.expect_error('SELECT pg_temp.insert_entry(''{"id":"l","transaction_id":"loss"}'')','23514');

-- Rollback of BET credits the exact original debit.
SELECT pg_temp.insert_wager('{"id":"rb","external_transaction_id":"rb","idempotency_key":"rb","kind":"ROLLBACK","reference_external_transaction_id":"ext","reference_transaction_id":"t","status":"PROCESSED","result_balance_minor":1000}');
SELECT pg_temp.expect_error('SELECT pg_temp.insert_entry(''{"id":"rb-entry","transaction_id":"rb","balance_before_minor":1100,"balance_after_minor":1000}'')','23514');
SELECT pg_temp.insert_entry('{"id":"rb-entry","transaction_id":"rb","direction":"CREDIT","balance_before_minor":900,"balance_after_minor":1000}');

-- A credit and its rollback exercise the opposite direction and missing original.
SELECT pg_temp.insert_wager('{"id":"win","external_transaction_id":"win","idempotency_key":"win","kind":"WIN","status":"PROCESSED","result_balance_minor":1100}');
SELECT pg_temp.insert_wager('{"id":"rw","external_transaction_id":"rw","idempotency_key":"rw","kind":"ROLLBACK","reference_external_transaction_id":"win","reference_transaction_id":"win","status":"PROCESSED","result_balance_minor":1000}');
SELECT pg_temp.expect_error('SELECT pg_temp.insert_entry(''{"id":"rw-entry","transaction_id":"rw","balance_before_minor":1100,"balance_after_minor":1000}'')','23514');
SELECT pg_temp.insert_entry('{"id":"win-entry","transaction_id":"win","direction":"CREDIT","balance_before_minor":1000,"balance_after_minor":1100}');
SELECT pg_temp.insert_entry('{"id":"rw-entry","transaction_id":"rw","balance_before_minor":1100,"balance_after_minor":1000}');

INSERT INTO wager_transactions(id,origin,wallet_id,player_id,currency,kind,amount_minor,status,result_balance_minor,created_at,updated_at)
 VALUES ('opening','INTERNAL','w','p','BRL','OPENING',1000,'PROCESSED',1000,'2026-10-14T09:00:00Z','2026-10-14T09:00:00Z');
SELECT pg_temp.insert_entry('{"id":"opening-entry","transaction_id":"opening","direction":"CREDIT","amount_minor":1000,"balance_before_minor":0,"balance_after_minor":1000,"created_at":"2026-10-14T09:00:00Z"}');

-- Exact arithmetic must reject an overflowing credit without overflowing its CHECK.
SELECT pg_temp.insert_wager('{"id":"huge","external_transaction_id":"huge","idempotency_key":"huge","kind":"WIN","amount_minor":9223372036854775807,"status":"PROCESSED","result_balance_minor":9223372036854775807}');
SELECT pg_temp.expect_error('SELECT pg_temp.insert_entry(''{"id":"huge-entry","transaction_id":"huge","direction":"CREDIT","amount_minor":9223372036854775807,"balance_before_minor":1,"balance_after_minor":9223372036854775807}'')','23514');
DO $$ BEGIN
 IF (SELECT count(*) FROM wallet_ledger_entries)<>5 OR
    (SELECT sum(CASE direction WHEN 'CREDIT' THEN amount_minor::NUMERIC ELSE -amount_minor::NUMERIC END) FROM wallet_ledger_entries)<>1000 THEN
  RAISE EXCEPTION 'Unexpected ledger contents';
 END IF;
END; $$;
ROLLBACK;
