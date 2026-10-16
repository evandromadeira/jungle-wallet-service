BEGIN;

CREATE FUNCTION pg_temp.expect_error(statement TEXT, expected_state TEXT)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE actual_state TEXT;
BEGIN
    BEGIN
        EXECUTE statement;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS actual_state = RETURNED_SQLSTATE;
        IF actual_state <> expected_state THEN
            RAISE EXCEPTION 'Expected SQLSTATE %, got % for %', expected_state, actual_state, statement;
        END IF;
        RETURN;
    END;
    RAISE EXCEPTION 'Expected SQLSTATE % but statement succeeded: %', expected_state, statement;
END;
$$;

INSERT INTO wallets (id, player_id, currency) VALUES ('w1', 'p1', 'BRL');
INSERT INTO wallets (id, player_id, currency, balance_minor)
VALUES ('w2', 'p1', 'USD', 9223372036854775807), ('w3', 'p2', 'BRL', 10000);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM wallets WHERE id = 'w1' AND balance_minor = 0 AND version = 1 AND created_at = updated_at) THEN
        RAISE EXCEPTION 'Initial defaults are incorrect';
    END IF;
END;
$$;

SELECT pg_temp.expect_error('INSERT INTO wallets(id,player_id,currency) VALUES(''other'',''p1'',''BRL'')', '23505');
SELECT pg_temp.expect_error('INSERT INTO wallets(id,player_id,currency) VALUES(''w1'',''p3'',''BRL'')', '23505');
SELECT pg_temp.expect_error('INSERT INTO wallets(id,player_id,currency) VALUES('''',''p3'',''BRL'')', '23514');
SELECT pg_temp.expect_error('INSERT INTO wallets(id,player_id,currency) VALUES(''w4'','' p3'',''BRL'')', '23514');
SELECT pg_temp.expect_error('INSERT INTO wallets(id,player_id,currency) VALUES(''w4'',NULL,''BRL'')', '23502');
SELECT pg_temp.expect_error('UPDATE wallets SET currency = ''ZZZ'' WHERE id = ''w1''', '23514');
SELECT pg_temp.expect_error('UPDATE wallets SET currency = ''brl'' WHERE id = ''w1''', '23514');
SELECT pg_temp.expect_error('UPDATE wallets SET balance_minor = -1 WHERE id = ''w1''', '23514');
SELECT pg_temp.expect_error('UPDATE wallets SET balance_minor = NULL WHERE id = ''w1''', '23502');
SELECT pg_temp.expect_error('UPDATE wallets SET version = 0 WHERE id = ''w1''', '23514');
SELECT pg_temp.expect_error('UPDATE wallets SET version = -1 WHERE id = ''w1''', '23514');
SELECT pg_temp.expect_error('UPDATE wallets SET version = NULL WHERE id = ''w1''', '23502');
SELECT pg_temp.expect_error('UPDATE wallets SET updated_at = created_at - INTERVAL ''1 second'' WHERE id = ''w1''', '23514');
SELECT pg_temp.expect_error('UPDATE wallets SET created_at = ''-infinity'' WHERE id = ''w1''', '23514');
SELECT pg_temp.expect_error('UPDATE wallets SET updated_at = ''infinity'' WHERE id = ''w1''', '23514');
SELECT pg_temp.expect_error('UPDATE wallets SET created_at = NULL WHERE id = ''w1''', '23502');
SELECT pg_temp.expect_error('UPDATE wallets SET balance_minor = balance_minor + 1 WHERE id = ''w2''', '22003');

UPDATE wallets SET balance_minor = 2000, version = version + 1 WHERE id = 'w3';
DO $$
BEGIN
    IF (SELECT count(*) FROM wallets) <> 3 OR
       NOT EXISTS (SELECT 1 FROM wallets WHERE id = 'w3' AND balance_minor = 2000 AND version = 2) OR
       NOT EXISTS (SELECT 1 FROM wallets WHERE id = 'w2' AND balance_minor = 9223372036854775807) THEN
        RAISE EXCEPTION 'Valid data was not preserved';
    END IF;
END;
$$;

ROLLBACK;
