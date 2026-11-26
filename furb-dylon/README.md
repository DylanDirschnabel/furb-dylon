# Furb Dylon

Trabalho final de Sistemas Distribuídos (FURB, 2026/2).

O aluno entra com o login do DION e ganha uma das 10 vagas de monitoramento. A partir daí o sistema raspa o DION de tempos em tempos e manda um e-mail quando aparece nota nova.

São 4 microsserviços em 3 linguagens, conversando por SNS e SQS. A AWS é emulada pelo MiniStack.

| Serviço | Linguagem | Porta | O que faz |
| --- | --- | --- | --- |
| inscricoes | Java 21, Spring Boot | 8081 | Inscrição, controle de vagas e a saga de validação |
| coletor-dion | Python, Flask | 8082 | Login e scraping do DION (requests + BeautifulSoup) |
| notas | Java 21, Spring Boot | sem porta (3 réplicas) | Compara com o que já existia e detecta notas novas |
| notificacao | Node 22, Express | 8084 | Envia os e-mails |

Além deles tem o `painel` (porta 8080), que é só a ferramenta da apresentação: tela de login, notas, filas, contêineres, logs e os botões dos cenários de falha F1 a F8.

## Como rodar

Precisa de Docker com Compose.

```bash
cp .env.example .env
docker compose up -d --build
```

Depois é só abrir http://localhost:8080.

No `.env`:

- `SMTP_*`: dados de um SMTP para o e-mail chegar de verdade. Com Gmail, crie uma senha de app em https://myaccount.google.com/apppasswords. Com `SMTP_HOST` vazio os e-mails ficam só no SES do MiniStack.
- `DION_MODE`: `real` raspa o DION de verdade; `simulado` usa notas inventadas (bom se o DION estiver fora do ar).

Usuários que começam com `f1`, `carga-`, `escala-` etc. sempre usam o DION simulado e nunca recebem e-mail real. São os usuários dos testes de falha.

## Infraestrutura

Tudo é criado pelo `infra/init_aws.py` quando o compose sobe.

- **SQS:** uma fila por serviço, cada uma com DLQ (3 tentativas).
- **SNS:** `inscricoes-eventos` e `notas-eventos`, com fan-out para as filas interessadas.
- **Secrets Manager:** a senha do DION de cada aluno (`dylon/aluno/<matricula>`).
- **S3:** cópia crua de cada raspagem (`dylon-snapshots`).
- **SES:** e-mails.

Um Postgres só, mas com um banco e um usuário por serviço (`infra/postgres-init.sql`).

| Fila | Recebe |
| --- | --- |
| inscricoes | `validacao.concluida` |
| coletor | `validacao.solicitada`, `inscricao.confirmada`, `inscricao.recusada` |
| notas | `notas.snapshot` |
| notificacao | `inscricao.confirmada`, `inscricao.recusada`, `notas.novas` |

Os contratos das mensagens estão em `contracts/`. Todas usam o mesmo envelope (`messageId`, `correlationId`, `type`, `version`, `occurredAt`, `payload`), e o `correlationId` é o id da inscrição.

## API

```bash
curl -i -X POST localhost:8081/inscricoes -H 'Content-Type: application/json' \
  -d '{"matricula":"f1-teste","senha":"123","email":"f1-teste@teste.invalid"}'

curl localhost:8081/inscricoes/<id>
curl localhost:8081/vagas
curl -X POST localhost:8082/coletar
curl localhost:8084/emails
```

O `POST /inscricoes` responde 202 com o id, e o status vai de `VALIDANDO` para `CONFIRMADA` ou `RECUSADA`.

## Onde está cada requisito

| Requisito | Onde |
| --- | --- |
| R1 Idempotência | Tabela `mensagens_processadas` em cada consumidor, gravada na mesma transação do efeito |
| R2 Concorrência | `UPDATE vagas SET ocupadas = ocupadas + 1 WHERE ocupadas < total` no `InscricaoService` |
| R3 API assíncrona | `POST /inscricoes` devolve 202, status consultado em `GET /inscricoes/{id}` |
| R4 Consumidores concorrentes | 3 réplicas do notas lendo a mesma fila |
| R5 Dependência instável | `services/coletor-dion/dion.py`; latência e taxa de falha ajustáveis pelo painel |
| R6 Reconexão | Loops de consumo com backoff e outbox que tenta publicar de novo |
| Saga e compensação | `InscricaoService`: reserva a vaga, pede validação e libera a vaga se der erro ou passar de 15 s |
