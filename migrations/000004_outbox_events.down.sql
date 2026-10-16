BEGIN;
DROP TABLE public.outbox_events;
DROP FUNCTION public.protect_outbox_event();
DROP FUNCTION public.reject_outbox_removal();
COMMIT;
