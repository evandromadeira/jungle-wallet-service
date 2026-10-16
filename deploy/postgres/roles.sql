\set ON_ERROR_STOP on
\set migrator ''
\set worker ''
\set migrator_password ''
\set worker_password ''
\getenv migrator WALLET_MIGRATOR_USER
\getenv worker WALLET_WORKER_USER
\getenv migrator_password WALLET_MIGRATOR_PASSWORD
\getenv worker_password WALLET_WORKER_PASSWORD
SELECT :'migrator' ~ '^[a-z][a-z0-9_]{0,62}$' AND :'worker' ~ '^[a-z][a-z0-9_]{0,62}$'
 AND :'migrator' NOT LIKE 'pg_%' AND :'worker' NOT LIKE 'pg_%'
 AND :'migrator' <> :'worker' AND current_user NOT IN (:'migrator',:'worker')
 AND length(:'migrator_password')>0 AND length(:'worker_password')>0 AS valid \gset
\if :valid
\else
 \echo 'Invalid dedicated role names or missing passwords.'
 DO $$ BEGIN RAISE EXCEPTION 'Database access provisioning refused'; END $$;
\endif
BEGIN;
-- Serialize provisioning with the migration runner in this database.
SELECT pg_advisory_xact_lock(719836421004);
SELECT format('CREATE ROLE %I LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L', :'migrator', :'migrator_password')
 WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname=:'migrator') \gexec
SELECT format('CREATE ROLE %I LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L', :'worker', :'worker_password')
 WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname=:'worker') \gexec
-- Existing roles are never silently demoted, adopted from memberships, or password-rotated.
SELECT NOT EXISTS (SELECT FROM pg_roles r WHERE r.rolname IN (:'migrator',:'worker')
 AND (r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls OR r.rolinherit OR NOT r.rolcanlogin
 OR EXISTS(SELECT FROM pg_auth_members m WHERE m.member=r.oid)))
 AND NOT EXISTS (SELECT FROM pg_shdepend d JOIN pg_roles r ON r.oid=d.refobjid
 WHERE r.rolname=:'worker' AND d.deptype='o') AS safe \gset
\if :safe
\else
 \echo 'Existing roles have elevated capabilities, memberships, or worker ownership. Refusing changes.'
 ROLLBACK;
 DO $$ BEGIN RAISE EXCEPTION 'Database access provisioning refused'; END $$;
\endif
-- This setup is for a dedicated application database, not a shared database.
SELECT format('REVOKE ALL ON DATABASE %I FROM PUBLIC',current_database()) \gexec
SELECT format('REVOKE ALL ON DATABASE %I FROM %I,%I',current_database(),:'migrator',:'worker') \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO %I,%I',current_database(),:'migrator',:'worker') \gexec
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
SELECT format('REVOKE ALL ON SCHEMA public FROM %I',:'worker') \gexec
SELECT format('GRANT USAGE,CREATE ON SCHEMA public TO %I',:'migrator') \gexec
SELECT format('GRANT USAGE ON SCHEMA public TO %I',:'worker') \gexec
-- Per-schema defaults only add to global defaults; revoke the built-in grant globally.
-- This affects future functions created by this role in this database, not existing functions.
SELECT format('ALTER DEFAULT PRIVILEGES FOR ROLE %I REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC',:'migrator') \gexec
-- Also undo any earlier schema-specific grant in the application's schema.
SELECT format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC',:'migrator') \gexec
COMMIT;
