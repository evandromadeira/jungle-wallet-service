BEGIN;
DROP TABLE public.wager_transactions;
DROP FUNCTION public.protect_wager_transaction();
ALTER TABLE public.wallets DROP CONSTRAINT wallets_identity_currency_unique;
COMMIT;
