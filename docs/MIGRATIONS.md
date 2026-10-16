# Reversão manual de migrations em banco descartável

O CLI não possui down. Este procedimento é destrutivo, somente para uma base descartável já migrada pelo runner. Pare todos os workers que a utilizam; guarde backup se houver dados a preservar. Não use no banco de desenvolvimento como rotina de reinício.

Exemplo: remover apenas a versão 5 (inbox) pelo container postgres do projeto escolhido. O SQL abaixo usa a identidade administrativa apenas para SET ROLE para o proprietário; não altera ownership nem desabilita triggers. Ajuste o projeto e a role antes de executar. Para reverter outras versões, prossiga estritamente em ordem descendente e altere arquivo/versão. Não remova uma versão enquanto houver versões posteriores.

Comandos em shell POSIX, na raiz do checkout:

```sh
docker compose -p SEU_PROJETO -f compose.yaml -f compose.app.yaml stop app
{
  printf '%s\n' 'BEGIN;' 'SET LOCAL ROLE jungle_migrator;' 'SELECT pg_advisory_xact_lock(719836421004);'
  printf '%s\n' 'DO $guard$ BEGIN IF (SELECT max(version) FROM public.schema_migrations) IS DISTINCT FROM 5 THEN RAISE EXCEPTION USING MESSAGE = $msg$Expected top version 5$msg$; END IF; END $guard$;'
  tr -d '\r' < migrations/000005_inbox_messages.down.sql | sed -e '/^BEGIN;$/d' -e '/^COMMIT;$/d'
  printf '%s\n' 'DELETE FROM public.schema_migrations WHERE version=5;' 'COMMIT;'
} | docker compose -p SEU_PROJETO exec -T postgres sh -c 'exec psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

Se qualquer instrução falhar, a conexão fecha sem COMMIT e a transação é revertida. Não execute aplicação com schema parcial. Reaplicar up e grants pelo overlay restaura o schema; dados removidos pelo down não voltam.

Após reverter todas as cinco versões, a tabela schema_migrations vazia pode permanecer. O runner consegue aplicar novamente a partir da versão 1. A reversão de migrations por SQL é exercitada nos testes de integração; este procedimento administrativo composto não foi executado no banco de desenvolvimento.