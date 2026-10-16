\set ON_ERROR_STOP on
\set migrator ''
\set worker ''
\getenv migrator WALLET_MIGRATOR_USER
\getenv worker WALLET_WORKER_USER
BEGIN;
SELECT pg_advisory_xact_lock(719836421004);
-- Require dedicated, nonprivileged roles and objects created by the migration identity.
SELECT count(*)=2 AND bool_and(NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolreplication AND NOT rolbypassrls AND NOT rolinherit AND rolcanlogin)
 AND NOT EXISTS(SELECT FROM pg_auth_members m JOIN pg_roles r ON r.oid=m.member WHERE r.rolname IN (:'migrator',:'worker'))
 AS safe FROM pg_roles WHERE rolname IN (:'migrator',:'worker') \gset
\if :safe
\else
 \echo 'Run role provisioning with dedicated identities first.'
 ROLLBACK;
 DO $$ BEGIN RAISE EXCEPTION 'Database access provisioning refused'; END $$;
\endif
SELECT count(*)=6 AND bool_and(pg_get_userbyid(c.relowner)=:'migrator') AS owned
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='public' AND c.relname IN ('wallets','wager_transactions','wallet_ledger_entries','outbox_events','inbox_messages','schema_migrations') AND c.relkind='r' \gset
\if :owned
\else
 \echo 'Required tables must exist and belong to the migration role. Automatic ownership transfer is disabled.'
 ROLLBACK;
 DO $$ BEGIN RAISE EXCEPTION 'Database access provisioning refused'; END $$;
\endif
-- Reset table and column grants, including grants left by previous configurations.
SELECT format('REVOKE ALL ON TABLE public.%I FROM PUBLIC,%I',c.relname,:'worker')
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='public' AND c.relname IN ('wallets','wager_transactions','wallet_ledger_entries','outbox_events','inbox_messages','schema_migrations') \gexec
SELECT format('REVOKE SELECT(%1$I),INSERT(%1$I),UPDATE(%1$I),REFERENCES(%1$I) ON TABLE public.%2$I FROM PUBLIC,%3$I',a.attname,c.relname,:'worker')
 FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='public' AND c.relname IN ('wallets','wager_transactions','wallet_ledger_entries','outbox_events','inbox_messages','schema_migrations') AND a.attnum>0 AND NOT a.attisdropped \gexec
SELECT format('GRANT SELECT,INSERT ON TABLE public.wallets,public.wager_transactions,public.wallet_ledger_entries,public.outbox_events,public.inbox_messages TO %I',:'worker') \gexec
SELECT format('GRANT UPDATE(balance_minor,version,updated_at) ON public.wallets TO %I',:'worker') \gexec
SELECT format('GRANT UPDATE(reference_transaction_id,status,failure_code,result_balance_minor,updated_at,attempts,next_attempt_at) ON public.wager_transactions TO %I',:'worker') \gexec
SELECT format('GRANT UPDATE(attempts,next_attempt_at,last_error,published_at) ON public.outbox_events TO %I',:'worker') \gexec
SELECT format('GRANT UPDATE(completed_at) ON public.inbox_messages TO %I',:'worker') \gexec
-- Ledger mutation/removal, history access, DDL and trigger disabling are not granted.
COMMIT;
