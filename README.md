# Jungle Wallet Service

Serviço em Go 1.27.2, PostgreSQL, SQS e OIDC. HTTP e consumidor compartilham processamento transacional, ledger imutável, inbox/outbox e idempotência persistente.

## Executar do zero

Requisitos: Docker Engine/Desktop com containers Linux e Docker Compose v2 atualizado. Go 1.27.2 é necessário apenas para testes/execução no host. Execute na raiz do projeto.

Copie `.env.example` para `.env` (`Copy-Item .env.example .env` no PowerShell, `cp .env.example .env` no shell). Os valores são públicos e exclusivos do ambiente local.

```sh
docker compose -f compose.yaml -f compose.app.yaml up -d --build --wait
docker compose -f compose.yaml -f compose.app.yaml ps -a
curl http://127.0.0.1:8080/health/ready
```

Esse é o equivalente completo a `docker compose up --build`. O overlay adiciona aplicação e inicialização portátil, sem PowerShell: Postgres saudável → roles dedicadas → migrations pelo migrador → grants restritos → app. LocalStack cria as filas e Keycloak importa o realm. A imagem compila wallet-worker e wallet-migrate e executa como UID 10001, sem root.

O Compose base contém somente dependências e é usado pelos testes no host. Não misture os dois modos no mesmo projeto: o overlay muda os endereços anunciados por OIDC/SQS. Para coexistência, use `-p` com nome distinto e altere portas.

Portas publicadas somente em 127.0.0.1: app 8080, Postgres 5433, SQS 4566 e Keycloak 8081. O app escuta 0.0.0.0 dentro do container.

```sh
docker compose -f compose.yaml -f compose.app.yaml stop
```

Esse comando preserva dados. Não use `down -v` para reiniciar. O comando de subida pode ser repetido; senhas existentes não são rotacionadas automaticamente. Schema legado sem histórico ou com dono diferente não é adotado silenciosamente.

## Variáveis

Compose lê `.env`; executáveis Go leem somente ambiente do processo.

- `POSTGRES_DB/USER/PASSWORD`: administrador, usado somente no provisionamento.
- `WALLET_MIGRATOR_USER/PASSWORD` e `WALLET_WORKER_USER/PASSWORD`: migrador proprietário e worker restrito.
- `POSTGRES_PORT`, `SQS_PORT`, `KEYCLOAK_PORT`, `WALLET_HTTP_PORT`: portas do host.
- `WALLET_DATABASE_URL` e `WALLET_MIGRATION_DATABASE_URL`: conexões separadas.
- `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` opcional: credenciais explícitas; não há fallback test/test no código.
- `WALLET_SQS_ENDPOINT`, `WALLET_INPUT_QUEUE_URL`, `WALLET_OUTPUT_QUEUE_URL`: mesmo endpoint; entrada FIFO e saída standard.
- `WALLET_PROVIDER_ID`: provedor confiável associado à fila; `WALLET_CONSUMER_NAME`: identidade estável da inbox.
- `WALLET_HTTP_ADDRESS`, `WALLET_OIDC_ISSUER`, `WALLET_OIDC_AUDIENCE`: habilite os três ou nenhum.

Overlay usa DNS interno. Issuer fixo: `http://keycloak:8080/realms/jungle-wallet`. Obter token pela porta publicada funciona: ele contém esse issuer interno, verificado pela aplicação. Os exemplos não precisam resolver keycloak no host. No Compose base, issuer é o endereço localhost publicado.

Credenciais são estáticas durante a execução; rotacione recriando o processo. Não há cadeia IAM/ECS automática. HTTPS é aceito na configuração, mas a validação foi local, sem AWS.

## Exemplos autenticados (PowerShell)

Não imprima tokens. Segredos abaixo pertencem somente ao realm de teste.

```powershell
$base = 'http://127.0.0.1:8080'
$idp = 'http://127.0.0.1:8081/realms/jungle-wallet/protocol/openid-connect/token'
$internalToken = (Invoke-RestMethod -Method Post -Uri $idp -Body @{
 grant_type='client_credentials'; client_id='wallet-internal'; client_secret='local-wallet-internal-only'
}).access_token
$providerToken = (Invoke-RestMethod -Method Post -Uri $idp -Body @{
 grant_type='client_credentials'; client_id='provider-a'; client_secret='local-provider-a-only'
}).access_token
$internalHeaders = @{Authorization="Bearer $internalToken"}
$providerHeaders = @{Authorization="Bearer $providerToken"; 'Idempotency-Key'='example-bet-1'}
$wallet = Invoke-RestMethod -Method Post "$base/wallets" -Headers $internalHeaders -ContentType 'application/json' -Body '{"playerId":"example-player","initialBalance":{"amount":"100.00","currency":"BRL"}}'
$body = @{
 providerId='provider-a'; externalTransactionId='example-bet-1'; playerId='example-player'
 walletId=$wallet.id; roundId='round-1'; gameId='game-1'; kind='BET'
 money=@{amount='25.00';currency='BRL'}
} | ConvertTo-Json -Depth 4
$bet = Invoke-RestMethod -Method Post "$base/wagering/transactions" -Headers $providerHeaders -ContentType 'application/json' -Body $body
$replay = Invoke-RestMethod -Method Post "$base/wagering/transactions" -Headers $providerHeaders -ContentType 'application/json' -Body $body
Invoke-RestMethod "$base/wallets/$($wallet.id)" -Headers $internalHeaders
$page = Invoke-RestMethod "$base/wallets/$($wallet.id)/ledger?limit=1" -Headers $internalHeaders
if ($page.nextCursor) {
 Invoke-RestMethod "$base/wallets/$($wallet.id)/ledger?limit=1&cursor=$($page.nextCursor)" -Headers $internalHeaders
}
Invoke-RestMethod "$base/wagering/transactions/$($bet.transactionId)" -Headers $providerHeaders
Invoke-RestMethod "$base/providers/provider-a/wagering/transactions/example-bet-1" -Headers $providerHeaders
Invoke-RestMethod -Method Post "$base/wallets/$($wallet.id)/reconciliation" -Headers $internalHeaders
Invoke-RestMethod "$base/metrics" -Headers $internalHeaders
Invoke-RestMethod "$base/health/live"
Invoke-RestMethod "$base/health/ready"
```

Resultado esperado desse exemplo: abertura HTTP 201 com saldo 100.00; aposta HTTP 200, status PROCESSED, saldo 75.00 e idempotentReplay=false; repetição HTTP 200 com o mesmo transactionId, saldo 75.00 e idempotentReplay=true. As consultas retornam 200. O ledger com limit=1 retorna primeiro CREDIT de 100.00 com nextCursor e depois DEBIT de 25.00 sem próximo cursor. Reconciliação retorna consistent=true, checkedEntries=2 e difference.amount=0.00. Métricas retornam texto Prometheus; health retorna status live/ready.

Repetir aposta com mesmo payload retorna replay com saldo histórico. Nova execução da abertura exige outro jogador ou reutilização da carteira. Tokens dos clients dos exemplos expiram em cinco minutos. O client provider-expiry-test é exclusivo da integração e expira em cinco segundos. Segundo provedor: provider-b / local-provider-b-only.

Dinheiro usa strings decimais BRL/USD. BET/WIN/REFUND/ROLLBACK exigem positivo; LOSS exige zero. REFUND/ROLLBACK exigem referenceExternalTransactionId. OPENING é interno.

Conferência manual em 16/10/2026, em projeto Docker exclusivo iniciado sem dados financeiros anteriores e usando .env.example: tokens, abertura, aposta, replay, consultas, paginação, reconciliação, métricas e health corresponderam aos exemplos. Também foram exercitados 202, 400, 401, 403, 404, 409, 413, 415 e 422. Os caminhos de falha 500/503 não foram induzidos manualmente nessa conferência. O comando wallet-migrate status retornou as cinco migrations aplicadas; peers adicionais e o fluxo alternativo pelo host também iniciaram corretamente.

### Respostas

Abertura: 201; jogador/moeda existente: 409. Aposta: 200 processada/replay, 202 pendente de referência, 422 rejeitada, 500 FAILED. Consultas e reconciliação: 200, inclusive auditoria divergente, sem reparar valores.

400 entrada inválida; 401 sem identidade válida; 403 proibido; 404 ausente ou transação alheia por ID; 409 conflito de payload; 413 corpo acima de 16 KiB; 415 mídia incompatível; 503 infraestrutura indisponível. Retente resultado ambíguo somente preservando chave e payload.

Handlers usam JSON como `{"error":"invalid_request"}`, `{"error":"idempotency_conflict"}` ou `{"error":"processing_unavailable"}`. Middleware de autenticação usa texto unauthorized/forbidden; roteamento padrão também pode retornar texto. Resposta de aposta contém transactionId, status, balance/failureCode quando disponíveis e idempotentReplay. Métricas usam texto Prometheus.

## Migrations

Overlay executa up automaticamente com migrador; worker não executa DDL.

```sh
docker compose -f compose.yaml -f compose.app.yaml run --rm migrate wallet-migrate status
```

CLI oferece somente up/status. [Procedimento manual de reversão](docs/MIGRATIONS.md). Reversões `*.down.sql` são manuais e destrutivas para banco descartável. Pare a aplicação, execute como proprietário em ordem 000005 → 000001 e remova cada versão correspondente de schema_migrations na mesma transação da reversão. Remova wrappers BEGIN/COMMIT do arquivo antes de combinar com a alteração do histórico; não deixe histórico aplicado para tabela removida. Testes validam up/down em banco descartável. Não há rollback automático de produção; prefira correção forward e backup.

No host: exporte variáveis de .env.example, execute `scripts/Set-DatabaseAccess.ps1 -Phase Roles`, `go run ./cmd/wallet-migrate up`, `scripts/Set-DatabaseAccess.ps1 -Phase Grants` e `go run ./cmd/wallet-worker`. Docker é o fluxo portátil recomendado.

## Testes e falhas

Use o Compose base com endpoints do host:

Se o Keycloak já estava em execução antes de atualizar o realm, recrie somente esse container com `docker compose up -d --force-recreate --wait keycloak` (mesmo projeto e portas). O realm local não tem volume persistente; isso importa o client provider-expiry-test exigido pelo teste de expiração. Não apague o volume do PostgreSQL.

```sh
docker compose up -d --wait postgres localstack keycloak
gofmt -l .
go vet ./...
go vet -tags=integration ./...
go test -count=1 ./...
go test -tags=integration -count=1 -timeout 10m ./...
go test -tags=integration -count=1 -run 'TestMultiprocessFinancialRecovery|TestTransportProcessRecovery|TestConcurrentReconciliation' -v ./tests/integration
```

**Porta SQS alternativa no Compose base:** há uma limitação reproduzida com SQS_PORT=24566: awslocal usa LOCALSTACK_HOST e tenta a porta publicada dentro do container, fazendo o provisionamento e o healthcheck falharem. A porta interna continua sendo 4566. O overlay completo usa DNS interno e não apresentou essa falha. Até corrigir o Compose, para testar o modo base com outra porta, use um arquivo de override local (por exemplo, compose.local-test.yaml):

```yaml
services:
  localstack:
    environment:
      AWS_ENDPOINT_URL: http://127.0.0.1:4566
```

Suba com `docker compose -p SEU_PROJETO -f compose.yaml -f compose.local-test.yaml up -d --wait postgres localstack keycloak`, depois exporte `COMPOSE_PROJECT_NAME=SEU_PROJETO` no terminal dos testes (PowerShell: `$env:COMPOSE_PROJECT_NAME='SEU_PROJETO'`). Mantenha as mesmas variáveis de portas. No runner, substitua também o valor de `-e COMPOSE_PROJECT_NAME`. Esse contorno muda apenas o destino interno do CLI; os endpoints anunciados aos testes continuam usando a porta publicada.

Integrações criam e limpam bancos/roles/filas exclusivos. Não migram o banco financeiro de desenvolvimento. Nunca aponte os testes para produção.

Race no Windows sem CGO, em PowerShell:

```powershell
docker run --rm --mount "type=bind,source=${PWD},target=/src,readonly" -w /src golang:1.27.2 go test -race -count=1 ./...
```

Esse comando cobre testes sem tag integration. Para incluir a integração, o runner abaixo contém Go, CGO, Docker CLI e Compose:

```powershell
docker compose up -d --wait postgres localstack keycloak
docker build -f deploy/Dockerfile.race -t jungle-wallet-race .
docker run --rm --network host --mount "type=bind,source=${PWD},target=/src,readonly" --mount type=bind,source=/var/run/docker.sock,target=/var/run/docker.sock -e COMPOSE_PROJECT_NAME=jungle-wallet-service jungle-wallet-race
```

Validação em 16/10/2026: todos os pacotes passaram com race; integração em 121,521s, sem corrida detectada. A suíte no host passou em 136,478s. Esta execução inclui reavaliação de referência após TTL, FAILED por ledger obrigatório ausente e rejeição de token expirado emitido pelo Keycloak. As dependências usaram projeto isolado e portas alternativas, com o contorno do awslocal descrito acima; a subida original do Compose base com porta SQS alternativa falhou antes desse contorno.

O comando padrão do runner é `go test -race -tags=integration -count=1 -timeout=15m ./...`. Requer host networking funcional (Linux nativo ou suporte habilitado no Docker Desktop), pois os testes descobrem portas publicadas em 127.0.0.1. Use o projeto Compose base, não o overlay da aplicação. Se personalizar o nome do projeto, passe o mesmo COMPOSE_PROJECT_NAME ao runner e suba as dependências com esse nome.

O socket fornece controle do Docker local ao runner; execute somente com código de teste confiável. A montagem read-only protege os arquivos do checkout, não restringe as ações da API Docker. Não use esse runner em um daemon de produção. Os fixtures continuam criando recursos exclusivos e não aplicam migrations ao banco financeiro de desenvolvimento.

A prova multiprocesso inicia três filhos com pools próprios, disputa saldo e repete identidade. Injeta kill antes/depois do commit, antes do ACK e antes da confirmação da outbox. Usa adapters reais em processos de teste, não três servidores Fx completos. Separadamente, TestWorkerSIGTERM compila e inicia o binário real, comprova cancelamento/rollback, liberação da mensagem e recuperação após reinício. Esse teste requer Linux/CGO e roda no runner Docker; é excluído nos testes nativos do Windows.

Workers adicionais sem publicar outra porta:

```sh
docker compose -f compose.yaml -f compose.app.yaml run -d --no-deps --name wallet-peer-2 -e WALLET_HTTP_ADDRESS= -e WALLET_OIDC_ISSUER= -e WALLET_OIDC_AUDIENCE= app
docker compose -f compose.yaml -f compose.app.yaml run -d --no-deps --name wallet-peer-3 -e WALLET_HTTP_ADDRESS= -e WALLET_OIDC_ISSUER= -e WALLET_OIDC_AUDIENCE= app
docker stop wallet-peer-2 wallet-peer-3
```

Os peers de run são temporários; não equivalem a um deployment supervisionado. Testes automatizados acima são a evidência de concorrência.

## Operação e limites

Falha de worker encerra o runtime; Compose usa restart: unless-stopped. Healthcheck sozinho não reinicia container: a saída do processo aciona restart. Fora do Compose é necessário supervisor. Não há ACK antes do commit; outbox pode reenviar o mesmo eventId, exigindo deduplicação externa.

Limites adicionais: o teste cruzado HTTP/SQS é sequencial, sem prova de entradas simultâneas; falha persistente de um candidato pode interromper repetidamente o lote do worker de referências. Na entrada SQS, campos adicionais dentro de money são ignorados, embora valor e moeda sejam validados. Veja os detalhes em [ARCHITECTURE.md](ARCHITECTURE.md).

LocalStack/Keycloak usam configuração de desenvolvimento. Não há TLS, persistência do IdP ou infraestrutura AWS provisionada. Tracing, dashboards e carga são opcionais e não implementados. Veja [ARCHITECTURE.md](ARCHITECTURE.md).

Políticas de acesso SQS por papel e validação pendente de enforcement: [BROKER_AUTHORIZATION.md](docs/BROKER_AUTHORIZATION.md). O Compose local com test/test não comprova controle de acesso do broker.

Contratos de saída, exemplos JSON e regras de consumo: [Eventos](docs/EVENTS.md).
