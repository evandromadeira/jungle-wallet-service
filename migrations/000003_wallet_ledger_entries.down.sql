BEGIN;
DROP TABLE public.wallet_ledger_entries;
DROP FUNCTION public.validate_wallet_ledger_entry();
DROP FUNCTION public.reject_wallet_ledger_mutation();
COMMIT;
