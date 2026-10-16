# Autorização do broker

## Artefatos e aplicação

deploy/broker contém templates de políticas IAM/resource policy. Não são políticas já implantadas. Substitua REGION, ACCOUNT_ID, PRODUCER_ROLE_ARN e WORKER_ROLE_ARN por valores do ambiente. Cada provedor deve ter fila e principal próprios; replique o binding sem compartilhar credenciais entre provedores.

- producer-policy.json: anexar somente à role do produtor confiável do provedor; permite SendMessage na fila de entrada.
- worker-policy.json: anexar à role do processo wallet-worker. Como o binário executa consumer e publisher, a role reúne consumo da entrada, publicação da saída e leitura dos atributos da DLQ.
- input-queue-policy.json: aplicar no atributo Policy da fila de entrada. Nega envio e consumo a principals não autorizados, mesmo quando outra política identity-based concede essas ações.

O provisionador tem identidade separada e privilégios de IAM/administrar filas. Nenhuma credencial real deve ser armazenada no repositório. Configure AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY e, para credenciais temporárias, AWS_SESSION_TOKEN no processo. O runtime atual recebe credenciais explícitas e não faz AssumeRole nem rotação automática; a obtenção e renovação são responsabilidade do ambiente. Os templates pressupõem roles do mesmo account e não substituem trust policies nem o provisionamento das roles.

A resource policy usa o ARN da role em aws:PrincipalArn, não o ARN de uma sessão STS. A role do worker não envia na entrada e o produtor não lê/apaga mensagens. Não conceda outras políticas amplas a essas roles. A aplicação ainda valida providerId contra a configuração confiável da fila.

## Evidência e limitação local

O Compose padrão usa LocalStack com test/test e não comprova isolamento entre principals. Uma verificação em fila descartável aplicou Deny explícito para sqs:SendMessage e o envio ainda foi aceito. Portanto, armazenar uma Policy no emulador não significa que ela esteja sendo aplicada.

Os templates desta pasta foram verificados como JSON; não foram implantados ou validados em AWS. O requisito de enforcement permanece pendente até executar testes allow/deny em AWS ou em um ambiente LocalStack que suporte e habilite IAM enforcement. Não basta adicionar ENFORCE_IAM=1 e presumir que a imagem oferece esse recurso.

Validação necessária com credenciais distintas:
1. Produtor autorizado envia uma mensagem válida na sua fila.
2. Outro produtor é negado nessa fila.
3. Produtor não consegue ReceiveMessage, DeleteMessage ou ChangeMessageVisibility.
4. Worker recebe, confirma e libera na entrada e publica na saída.
5. Worker não consegue enviar na entrada nem alterar Policy/RedrivePolicy.
6. Credencial ausente ou inválida é rejeitada.
7. Mensagem aceita pelo broker mas com providerId divergente não movimenta saldo e segue o tratamento de inválidas/DLQ.

Fontes:
- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-using-identity-based-policies.html
- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-creating-custom-policies-evaluation-logic.html
- https://docs.localstack.cloud/aws/developer-tools/security-testing/iam-policy-enforcement/
