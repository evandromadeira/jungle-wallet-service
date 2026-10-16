BEGIN;
-- Deliberately omit CASCADE: dependent financial tables must be reversed first.
DROP TABLE public.wallets;
COMMIT;
