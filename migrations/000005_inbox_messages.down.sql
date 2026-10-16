BEGIN;
DROP TABLE public.inbox_messages;
DROP FUNCTION public.protect_inbox_message();
DROP FUNCTION public.reject_inbox_removal();
COMMIT;
