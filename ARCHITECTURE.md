# Decisões de arquitetura

## Camadas e Fx

Domínio contém Money, Wallet, WagerTransaction e WalletLedgerEntry imutáveis, sem HTTP, SQL, broker ou Fx. Aplicação coordena transações e portas; adapters implementam PostgreSQL/SQS/OIDC.

Bootstrap usa módulos Fx de infraestrutura, aplicação e transporte: construtores injetam pool, SDK, store, casos de uso, consultas, workers e fábrica de rotas. OIDC discovery é adiado para OnStart; construção do grafo não requer rede e pool é lazy. Startup valida papel restrito, schema e filas.

Fx cancela workers, baixa readiness e drena HTTP até 15 segundos dentro do prazo global de 20. Recursos fecham depois das tarefas; hooks de infraestrutura fazem cleanup idempotente. Startup malsucedido também fecha recursos. Não há retry cego de commit.

## Dinheiro e carteira

Int64 em unidades menores, moeda explícita, sem float. JSON de entrada exige strings decimais canônicas não negativas. Internamente diferenças podem ser negativas e operações verificam overflow. Decoder externo de Money não deve reler diferença negativa de saída.

Carteira mantém saldo não negativo e versão; jogador/moeda é único. Abertura positiva cria OPENING, crédito no ledger e dois eventos no mesmo commit. Abertura zero cria apenas carteira versão 1. IDs são opacos validados, sem exigência adicional de UUID do cliente.

## Transações, locks e idempotência

READ COMMITTED e SELECT FOR UPDATE por carteira serializam movimentos. Identidades são verificadas antes e após lock. Carteiras distintas progridem independentemente. Saldo, transação, ledger, inbox e outbox são atômicos conforme o caso de uso.

Duas identidades únicas: (providerId,idempotencyKey) e (providerId,externalTransactionId). Hash SHA-256 cobre JSON com chaves de negócio ordenadas, Money canônico e referência ausente como string vazia. Exclui chave de idempotência, ID interno, timestamps, estado e metadados de transporte. IDs não são convertidos para minúsculas ou aparados silenciosamente.

Chave alternativa pode repetir identidade externa, sem registrar alias. Payload diferente conflita. Replay usa saldo histórico persistido. Inbox deduplica envelope/consumidor por hash dos bytes, não receipt SQS; JSON textualmente distinto no mesmo envelope pode conflitar mesmo com negócio equivalente.

## Referências e reversões

BET debita, WIN credita, LOSS zero não movimenta saldo/ledger. REFUND e ROLLBACK exigem referência compatível em provedor, jogador, carteira, moeda, rodada e valor. REFUND credita aposta; ROLLBACK inverte lançamento original e pode falhar por saldo insuficiente.

Há no máximo uma reversão bem-sucedida por alvo, considerando REFUND/ROLLBACK conjuntamente. Sem reversões parciais. Lock e restrições persistentes protegem concorrência. Referência ausente gera PENDING_REFERENCE; worker usa backoff, máximo de tentativas e TTL para resolução ou rejeição transacional. 202 representa espera persistida, não trabalho volátil.

## Ledger e reconciliação

Ledger append-only é protegido por constraints/triggers e privilégios, validando direção, vínculo e aritmética. Reconciliação usa um SELECT para ler carteira e somar créditos menos débitos incluindo OPENING na mesma visão. SUM numeric evita overflow intermediário; resultado/diferença ainda devem caber em Money. Não há reparo: divergência aparece na resposta, log e contador.

Paginação usa (created_at,id) crescente, índice por carteira, cursor base64url e limite 1–100 (padrão 50). Cursor não é autorização nem assinado. Cada consulta filtra carteira e exige identidade interna. Páginas não congelam snapshot; inserções anteriores ao cursor exigem reiniciar navegação. Timestamp não garante ordem de commit.

## Inbox, outbox e SQS

Entrada FIFO agrupada por carteira; lock SQL continua sendo garantia entre HTTP/SQS. Consumer recebe um item por vez, limita processamento/visibilidade e só remove após commit. Retry altera visibilidade com backoff; cancelamento tenta liberar receipt com contexto independente. Redrive move para DLQ após cinco recebimentos.

Outbox guarda envelope imutável dos eventos WagerTransactionProcessed, WagerTransactionRejected, WalletBalanceChanged e WagerTransactionPendingReference. Envelope inclui eventId, eventType, aggregateId, correlationId, causationId opcional, occurredAt UTC, version e data tipado.

Publisher usa FOR UPDATE SKIP LOCKED e envia somente registros já commitados. Confirma envio em transação; timeout de rede limita lock. Crash após aceitação e antes da confirmação causa reenvio com mesmo ID e conteúdo. Entrega é pelo menos uma vez: consumidor externo deduplica eventId. Saída standard única é roteada por eventType/version; consumidor externo não está implementado.

## Autenticação, credenciais e permissões

Keycloak fornece client credentials/OIDC, sem autenticação de senha de usuário na aplicação. Validação exige assinatura RS256, issuer exato, audience wallet-api, expiração, tipo e claims compatíveis. Claims fixos determinam provedor; corpo não escolhe autoridade.

Provedor só envia/consulta suas transações. Carteira, ledger, reconciliação e métricas exigem serviço interno. Health é público. Transação alheia por ID retorna 404; divergência explícita de provedor retorna 403.

Produtor SQS não usa token OIDC na aplicação: broker deve autorizar credencial à fila do provedor. WALLET_PROVIDER_ID é configuração confiável. Vários provedores exigem bindings/filas/workers separados.

Privilégios desejados:
- Produtor: SendMessage somente na fila de entrada autorizada.
- Consumer: ReceiveMessage, DeleteMessage, ChangeMessageVisibility, GetQueueAttributes na entrada.
- Publisher: SendMessage e GetQueueAttributes no destino.
- Coleta: GetQueueUrl/GetQueueAttributes da DLQ.
- Provisionador: CreateQueue/SetQueueAttributes; não é função do runtime.

Região/credenciais vêm do ambiente, com session token opcional; sem fallback de teste no código ou cadeia IAM automática. Rotação exige recriação. Compose fornece test/test somente para LocalStack. Políticas IAM reais não foram implantadas.

Banco separa administrador, migrador dono e worker não proprietário. Worker tem SELECT/INSERT e UPDATE somente em colunas mutáveis, sem DDL, alteração de ledger, histórico ou desativação de triggers. Default EXECUTE de funções é revogado globalmente para o migrador.

## Empacotamento e recuperação

Docker multi-stage Go 1.27.2 executa sem root. Compose base atende testes no host; overlay adiciona roles → migrations → grants → app e mantém dependências saudáveis como pré-condição. Provisionamento não adota schema legado sem histórico ou gira senhas existentes.

Escolha: fail-fast com supervisão externa. Overlay usa unless-stopped; saída aciona restart, não apenas healthcheck. Dados duráveis permitem retomada. Falhas permanentes podem gerar restart contínuo e exigem intervenção. Fora do Compose é preciso supervisor. Reinício não relaxa ACK após commit. O worker de referências interrompe o lote quando um candidato retorna erro; não há isolamento de falhas por candidato. Um candidato que falha persistentemente e volta a ser selecionado antes dos demais pode impedir o avanço das pendências seguintes, inclusive após reinícios, exigindo intervenção.

## Observabilidade

Métricas por processo cobrem resultados/status, duração count/sum (sem quantis), replay, conflitos, retries e divergências. IDs ficam nos logs JSON, não em labels; sem payload financeiro, tokens ou erros brutos.

Gauges sob demanda: backlog/idade de outbox e quantidades aproximadas visíveis/em voo na DLQ. Scrape tem três segundos; falha retorna 503, não zero falso. Quantidade DLQ não é total histórico. Contadores reiniciam com processo; crash após commit pode impedir emissão. Observabilidade não substitui ledger. Espera normal por lock não conta como conflito; latência HTTP exclui autenticação/decodificação.

Liveness não consulta dependências. Readiness verifica runtime, Ping PostgreSQL e atributos das filas, não permissões de envio, IdP, backlog ou integridade completa do schema.

## Evidências e limites

PostgreSQL/Keycloak reais e LocalStack, sem AWS. Três subprocessos com pools próprios disputam saldo/repetem operação. A disputa de duas apostas de 80 sobre saldo 100 verifica explicitamente uma PROCESSED e uma REJECTED/INSUFFICIENT_FUNDS. Um lote de 50 requisições idênticas, distribuído em 17/17/16 entre três processos, verifica uma transação, 49 replays e um único débito. Outro cenário libera três processos para carteiras distintas e verifica separadamente saldo, versão, resultado, ledger e outbox de cada carteira. Kill antes/depois do commit, antes do ACK e antes da confirmação comprova recuperação e estabilidade dos envelopes. Barreiras existem apenas nos testes.

Os testes de concorrência usam casos de uso/adapters reais, não três servidores Fx completos. Lifecycle Fx tem teste próprio com listener. O teste cruzado HTTP/SQS comprova replay por SQS após o commit HTTP, de forma sequencial. Ainda não há teste de entradas HTTP e SQS simultâneas para a mesma operação ou em disputa pelo saldo. TestWorkerSIGTERM (integration && linux) compila cmd/wallet-worker com -race, inicia o binário real com HTTP/OIDC e envia SIGTERM após confirmar via pg_blocking_pids que uma operação SQS aguarda um lock de carteira. Verifica saída zero em até dez segundos, fechamento do listener e conexões, rollback sem efeitos parciais em saldo/ledger/inbox/outbox, liberação da mensagem antes dos 60 segundos de visibilidade e ausência de nova busca de uma segunda mensagem elegível. Reinicia o binário e verifica ambas concluídas, dois débitos únicos, ACK e DLQ vazia. O caminho comprovado é cancelamento/rollback e reentrega; não conclusão da operação bloqueada durante o desligamento. SIGTERM simultâneo de múltiplos servidores e todas as possíveis janelas de interrupção não estão cobertos. O teste requer Linux/CGO (runner Docker); não é executado pelos testes nativos no Windows.

Race passou também com tag integration no runner Linux Go 1.27.2/CGO, Docker CLI e host networking: pacote de integração em 121,521s, incluindo os lotes em três subprocessos e o SIGTERM do binário real. Essa é evidência dos cenários executados, não prova de ausência de toda corrida possível. JSON duplicado segue encoding/json. Não há TLS local, persistência do IdP, tracing, dashboards, carga ou deduplicação externa.
## Persistência e fronteira transacional

O adapter PostgreSQL usa pgx/v5 e pgxpool. Money é convertido para BIGINT em unidades menores e currency TEXT, sem conversão intermediária para ponto flutuante. As consultas de reconciliação usam SUM numeric e verificam a conversão final para int64.

UnitOfWork abre uma transação READ COMMITTED e fornece repositórios ligados à mesma conexão. O callback compartilha saldo, versão, wager, ledger e outbox; HandleMessage inclui também a inbox. Só há resultado de sucesso após Commit. Erros causam rollback com contexto de cleanup independente. Falha de commit pode ser ambígua: não é classificada como FAILED nem repetida automaticamente.

## Estados e códigos de falha

PENDING é o estado inicial de domínio; a operação persiste PROCESSED, REJECTED, FAILED ou PENDING_REFERENCE atomicamente. PENDING_REFERENCE pode continuar esperando ou terminar em um desses três estados terminais. Estados terminais são imutáveis; replay conserva resultado e saldo históricos. Consulta de transação retorna HTTP 200 mesmo para resultado terminal negativo; os códigos abaixo referem-se ao POST da operação e seu replay.

- INSUFFICIENT_FUNDS: BET sem saldo. REJECTED, HTTP 422.
- REVERSAL_INSUFFICIENT_FUNDS: ROLLBACK que precisaria debitar mais que o saldo. REJECTED, HTTP 422.
- REFERENCE_NOT_FOUND: referência ausente ou ainda pendente após esgotar tentativas/TTL. REJECTED, HTTP 422.
- REFERENCE_UNSUCCESSFUL: referência existente em REJECTED ou FAILED. Rejeição imediata, HTTP 422.
- REFERENCE_MISMATCH: referência incompatível em identidade, operação ou valor. REJECTED, HTTP 422.
- ALREADY_REVERSED: referência já revertida com sucesso. REJECTED, HTTP 422.
- BALANCE_OVERFLOW: resultado não representável em Money. REJECTED, HTTP 422.
- INFRASTRUCTURE_FAILURE: integridade permanentemente inconsistente: ROLLBACK encontra referência financeira PROCESSED, mas uma consulta bem-sucedida confirma ausência de seu ledger obrigatório. FAILED, HTTP 500, sem movimentação de saldo ou ledger. O registro terminal preserva a auditoria e a inbox pode ser concluída; não é publicado como rejeição de negócio ou sucesso.

Esses resultados são definitivos para a identidade original: corrigir saldo ou infraestrutura não reaplica um replay. Problemas transitórios de consulta/conexão e commits ambíguos retornam erro, preservam retry e não gravam FAILED. Não há classificação genérica de todo erro de infraestrutura como permanente.

Referência ausente ou ainda pendente gera HTTP 202/PENDING_REFERENCE. O worker reavalia primeiro, sob lock da carteira, inclusive quando o TTL já passou; só rejeita por prazo/tentativas se ainda estiver esperando. A configuração permite 100 tentativas, atraso inicial 1 s com teto de 60 s e TTL de 24 h. Sob operação contínua, as tentativas normalmente esgotam antes do TTL (cerca de 95 minutos, mais processamento e polling). O TTL também limita pendências após indisponibilidade prolongada.

## Contrato de produção e consumo SQS

O produtor deve enviar MessageGroupId=walletId e MessageDeduplicationId=messageId do envelope. A deduplicação FIFO não substitui idempotência persistente: envelope/consumidor é deduplicado na inbox; identidade de negócio é deduplicada na wager. Cada fila é vinculada a um providerId confiável pela configuração do consumidor.

O runtime usa long polling de 10 s, um item por receive, visibilidade de 60 s, timeout de processamento de 30 s e backoff de 2 s dobrando até 60 s. O redrive configurado admite cinco recebimentos antes da DLQ. Mensagens inválidas ou com conflito de payload não são confirmadas; seguem retry/redrive. O decoder SQS rejeita campos desconhecidos no envelope e em data, exceto dentro de money: Money.UnmarshalJSON usa um decoder próprio que ignora campos adicionais. A validação de valor e moeda continua obrigatória. Não há envio manual direto à DLQ, que poderia separar envio e ACK em duas operações não atômicas. O grupo FIFO pode ficar bloqueado durante essas tentativas. Rejeição de negócio persistida é conclusão válida e permite ACK. SIGTERM tenta liberar o receipt com visibilidade zero em contexto de cleanup de até 5 s.

## Expiração OIDC

O realm local inclui provider-expiry-test, client exclusivo dos testes com token de cinco segundos e claims de provider-a. A integração obtém um token assinado pelo Keycloak, confirma autorização antes do vencimento, espera expirar sem alterar o token e exige HTTP 401. Os clients dos exemplos continuam com validade normal de cinco minutos.

Políticas por papel e o limite de enforcement local estão documentados em [BROKER_AUTHORIZATION.md](docs/BROKER_AUTHORIZATION.md). Os templates não constituem prova de autorização: o emulador atual aceitou um envio explicitamente negado em uma fila de teste.

Contratos de saída, exemplos JSON e regras de consumo: [Eventos](docs/EVENTS.md).
