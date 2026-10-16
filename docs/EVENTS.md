# Contrato dos eventos de saída

## Transporte e roteamento

O publisher envia o envelope JSON inteiro como MessageBody para a fila SQS standard configurada por WALLET_OUTPUT_QUEUE_URL (localmente, jungle-wallet-events). Não envia MessageAttributes, MessageGroupId ou MessageDeduplicationId. O consumidor deve ler eventType e version no corpo para escolher o tratamento; não há roteamento automático por atributo nem uma fila por tipo.

A entrega é pelo menos uma vez e não garante ordem, inclusive entre eventos da mesma carteira. Eventos da mesma transação financeira podem chegar separados ou fora de ordem. Reenvio da outbox preserva eventId e conteúdo, mas o MessageId do SQS pode mudar.

O consumidor externo não faz parte deste projeto. Para integrá-lo:
- Deduplicar por eventId, persistindo a deduplicação atomicamente com os efeitos locais antes de confirmar o receipt SQS.
- Não deduplicar por transactionId: uma mesma transação pode produzir eventos diferentes, cada um com eventId próprio.
- Validar eventType/version e o schema de data. Tipos ou versões não suportados precisam de tratamento explícito (por exemplo, quarentena); não devem ser interpretados como outro evento.
- Para projeções de saldo, usar walletVersion para detectar eventos antigos e lacunas. A fila não entrega um snapshot ordenado; consultar/reconciliar a carteira quando necessário.
- Não usar um evento de notificação como comando para debitar ou creditar novamente a carteira.

## Envelope comum, versão 1

- eventId: identidade única e estável do evento.
- eventType: um dos quatro tipos descritos abaixo.
- aggregateId: transactionId nos três eventos WagerTransaction; walletId em WalletBalanceChanged.
- correlationId: identificador obrigatório de correlação do processamento.
- causationId: identificador opcional da causa, omitido quando vazio; no fluxo SQS normalmente corresponde ao messageId do envelope de entrada.
- occurredAt: instante do evento em UTC, serializado em RFC 3339, podendo incluir frações de segundo. Não representa ordem de entrega.
- version: versão do contrato do evento, atualmente 1; não é a versão da carteira.
- data: objeto específico do evento. Não existe campo payload no envelope publicado.

Os IDs são opacos; os valores curtos dos exemplos são ilustrativos. Money tem amount como string decimal com duas casas e currency explícita (BRL ou USD).

## WagerTransactionProcessed

Notifica operação concluída em PROCESSED. data contém transactionId, walletId, playerId, origin, kind, money e balance (saldo histórico após a operação). Para operações externas, também contém providerId, externalTransactionId, roundId e gameId. referenceExternalTransactionId aparece apenas quando existe referência.

Exemplo de BET de 25.00 sobre saldo 100.00:

```json
{
  "eventId": "evt-bet-processed",
  "eventType": "WagerTransactionProcessed",
  "aggregateId": "tx-bet-1",
  "correlationId": "request-1",
  "causationId": "message-1",
  "occurredAt": "2026-10-16T08:00:00Z",
  "version": 1,
  "data": {
    "transactionId": "tx-bet-1",
    "walletId": "wallet-1",
    "playerId": "player-1",
    "origin": "EXTERNAL",
    "kind": "BET",
    "money": {"amount": "25.00", "currency": "BRL"},
    "providerId": "provider-a",
    "externalTransactionId": "bet-1",
    "roundId": "round-1",
    "gameId": "game-1",
    "balance": {"amount": "75.00", "currency": "BRL"}
  }
}
```

LOSS também produz este evento, com money.amount=0.00, sem movimento financeiro. A abertura positiva produz este evento com origin=INTERNAL e kind=OPENING; providerId, externalTransactionId, roundId, gameId e referência são omitidos.

## WagerTransactionRejected

Notifica rejeição de negócio terminal. data usa os mesmos campos de identidade/operação, acrescenta failureCode e não contém balance. Mesmo quando a resposta HTTP contém saldo, ele não é incluído neste tipo de evento.

```json
{
  "eventId": "evt-bet-rejected",
  "eventType": "WagerTransactionRejected",
  "aggregateId": "tx-bet-2",
  "correlationId": "request-2",
  "occurredAt": "2026-10-16T08:01:00Z",
  "version": 1,
  "data": {
    "transactionId": "tx-bet-2",
    "walletId": "wallet-1",
    "playerId": "player-1",
    "origin": "EXTERNAL",
    "kind": "BET",
    "money": {"amount": "80.00", "currency": "BRL"},
    "providerId": "provider-a",
    "externalTransactionId": "bet-2",
    "roundId": "round-2",
    "gameId": "game-1",
    "failureCode": "INSUFFICIENT_FUNDS"
  }
}
```

Códigos possíveis: INSUFFICIENT_FUNDS, REVERSAL_INSUFFICIENT_FUNDS, REFERENCE_NOT_FOUND, REFERENCE_UNSUCCESSFUL, REFERENCE_MISMATCH, ALREADY_REVERSED e BALANCE_OVERFLOW. As condições estão em [ARCHITECTURE.md](../ARCHITECTURE.md). INFRASTRUCTURE_FAILURE pertence a FAILED e não a este evento.

## WagerTransactionPendingReference

Notifica a primeira persistência de PENDING_REFERENCE. data contém identidade/operação e referenceExternalTransactionId, sem balance ou failureCode. Não significa processamento financeiro concluído. As tentativas do worker não geram uma nova notificação de pendência a cada execução.

```json
{
  "eventId": "evt-refund-pending",
  "eventType": "WagerTransactionPendingReference",
  "aggregateId": "tx-refund-1",
  "correlationId": "request-3",
  "causationId": "message-3",
  "occurredAt": "2026-10-16T08:02:00Z",
  "version": 1,
  "data": {
    "transactionId": "tx-refund-1",
    "walletId": "wallet-1",
    "playerId": "player-1",
    "origin": "EXTERNAL",
    "kind": "REFUND",
    "money": {"amount": "10.00", "currency": "BRL"},
    "providerId": "provider-a",
    "externalTransactionId": "refund-1",
    "roundId": "round-3",
    "gameId": "game-1",
    "referenceExternalTransactionId": "late-bet"
  }
}
```

A resolução posterior publica Processed ou Rejected com outro eventId e o mesmo transactionId, conforme o resultado. Uma falha terminal FAILED não tem evento de saída próprio na implementação atual.

## WalletBalanceChanged

Notifica uma movimentação efetiva registrada no ledger. data contém walletId, transactionId, direction (DEBIT ou CREDIT), money, balanceBefore, balanceAfter e walletVersion. Não contém playerId nem metadados de provedor.

Exemplo correspondente à aposta processada acima:

```json
{
  "eventId": "evt-wallet-debited",
  "eventType": "WalletBalanceChanged",
  "aggregateId": "wallet-1",
  "correlationId": "request-1",
  "causationId": "message-1",
  "occurredAt": "2026-10-16T08:00:00Z",
  "version": 1,
  "data": {
    "walletId": "wallet-1",
    "transactionId": "tx-bet-1",
    "direction": "DEBIT",
    "money": {"amount": "25.00", "currency": "BRL"},
    "balanceBefore": {"amount": "100.00", "currency": "BRL"},
    "balanceAfter": {"amount": "75.00", "currency": "BRL"},
    "walletVersion": 2
  }
}
```

Abertura positiva gera CREDIT com walletVersion=1. LOSS, rejeição, pendência e FAILED não geram WalletBalanceChanged. Abertura com saldo zero não gera os eventos financeiros de abertura.

## Atomicidade e replay

Os eventos são persistidos na outbox junto com a alteração financeira, na mesma transação SQL. Só são publicados após commit. Um replay HTTP/SQS não cria novos movimentos ou eventos para o mesmo resultado. Isso não impede que o publisher reenvie eventos já existentes após uma falha de confirmação.

Os nomes e campos deste documento correspondem aos construtores em internal/domain/events.go e ao envio em internal/adapters/sqs/sender.go. A inclusão de contratos futuros exige uma decisão explícita de compatibilidade e versionamento; não há negociação automática de schema.
