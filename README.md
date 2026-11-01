# Cronos

Agendador de jobs distribuído, escrito em Kotlin com Spring Boot e PostgreSQL.

Você cadastra jobs com um agendamento (cron, intervalo ou execução única) e o Cronos garante que cada ocorrência seja registrada **uma única vez**, mesmo com várias instâncias do scheduler rodando ao mesmo tempo e mesmo depois de o sistema ficar fora do ar.

> **Status:** todos os itens do [roadmap](#roadmap) estão prontos: scheduler, misfire, API, worker com lease/heartbeat/reaper, retentativas, timeout, cancelamento, disparo manual, métricas e testes de integração, caos e carga. O que falta para produção está em [Limitações conhecidas](#limitações-conhecidas).

## Sumário

- [Por que este projeto](#por-que-este-projeto)
- [Arquitetura](#arquitetura)
- [Como o scheduler funciona](#como-o-scheduler-funciona)
- [Política de misfire](#política-de-misfire)
- [Como o worker funciona](#como-o-worker-funciona)
- [Métricas](#métricas)
- [Segurança](#segurança)
- [Retenção do histórico](#retenção-do-histórico)
- [Stack](#stack)
- [Como rodar](#como-rodar)
- [API](#api)
- [Decisões de projeto](#decisões-de-projeto)
- [Estrutura do projeto](#estrutura-do-projeto)
- [Testes](#testes)
- [Desempenho](#desempenho)
- [Roadmap](#roadmap)
- [Limitações conhecidas](#limitações-conhecidas)

## Por que este projeto

O `CronJob` do Kubernetes e o `@Scheduled` do Spring resolvem o caso simples, mas deixam de fora o que dói em produção:

| Necessidade | `@Scheduled` | `CronJob` (K8s) | Cronos |
|---|---|---|---|
| Rodar uma vez só com várias réplicas | não (cada instância dispara) | sim | sim |
| Histórico de execuções consultável | não | limitado | sim (tabela `executions`) |
| Política para disparos perdidos | não | limitada | sim (`SKIP`, `FIRE_ONCE`, `FIRE_ALL`) |
| Pausar e retomar por API | não | editando o recurso | sim |
| Evitar sobreposição de execuções | não | `concurrencyPolicy` | sim (`allowOverlap`) |
| Fuso horário por job | não | por cluster | sim |

O objetivo é praticar problemas clássicos de sistemas distribuídos: coordenação sem lock central, deduplicação, recuperação de falhas e semântica de entrega.

## Arquitetura

```mermaid
flowchart LR
    Client([Cliente]) -->|REST| API
    subgraph Cronos
        API[API]
        Scheduler[Scheduler]
        Worker[Worker]
    end
    API --> DB[(PostgreSQL)]
    Scheduler --> DB
    Worker --> DB
    Worker -->|webhook| Target([Destino do job])
```

Os três componentes não se comunicam diretamente. Toda a coordenação acontece pelo PostgreSQL:

- **API** cria, consulta, pausa e retoma jobs.
- **Scheduler** transforma jobs vencidos em linhas na tabela `executions`.
- **Worker** reivindica execuções pendentes e as realiza chamando o webhook do job.

Como não há comunicação entre processos, dá para subir várias réplicas de cada um sem configuração extra.

### Modelo de dados

- `jobs`: definição do job (agendamento, payload, política de misfire, status) e o campo `next_fire_at`, que é o que o scheduler consulta.
- `executions`: uma linha por ocorrência de um job, com status, tentativas e dados de lease. A restrição `UNIQUE (job_id, scheduled_for)` impede duplicidade.

## Como o scheduler funciona

A cada tick (padrão: 1 segundo), numa única transação:

1. Seleciona um lote de jobs `ACTIVE` com `next_fire_at <= now()` usando `FOR UPDATE SKIP LOCKED`. Cada instância pega um conjunto diferente de jobs, sem esperar uma pela outra.
2. Para cada job, o `MisfireResolver` decide quais ocorrências devem virar execução e qual será o próximo `next_fire_at`. Isso acontece em memória.
3. Insere todas as execuções do lote num único batch, com `ON CONFLICT DO NOTHING`. Se outra instância já criou a mesma ocorrência, o insert é ignorado.
4. Atualiza o `next_fire_at` de todos os jobs do lote, também em batch (ou marca o job como `COMPLETED` se não houver mais ocorrências).
5. Se o lote veio cheio, repete imediatamente sem esperar o próximo tick.

Duas barreiras contra duplicidade, em camadas: o `SKIP LOCKED` evita que duas instâncias processem o mesmo job ao mesmo tempo, e a restrição `UNIQUE` garante a unicidade mesmo se a primeira falhar.

Um job com agendamento inválido é pausado e registrado em log, em vez de travar o lote inteiro.

## Política de misfire

*Misfire* é um disparo que ficou atrasado além da tolerância do job (`misfireThresholdSeconds`, padrão 60). Isso acontece, por exemplo, quando o sistema ficou fora do ar. O que fazer com o tempo perdido é configurável por job.

Exemplo: job que roda a cada minuto, sistema parado das 10:00 às 10:05:30.

| Política | Comportamento | Resultado no exemplo |
|---|---|---|
| `SKIP` | Ignora o que foi perdido e segue na próxima ocorrência futura | nenhuma execução; próximo disparo às 10:06 |
| `FIRE_ONCE` (padrão) | Cria uma única execução de recuperação | 1 execução (10:00); próximo às 10:06 |
| `FIRE_ALL` | Recupera todas as ocorrências perdidas, com limite de 100 por rodada | 6 execuções (10:00 a 10:05); próximo às 10:06 |

Se o limite de 100 for atingido no `FIRE_ALL`, o `next_fire_at` aponta para a próxima ocorrência ainda perdida e o tick seguinte continua de onde parou.

## Como o worker funciona

O worker roda na mesma aplicação (pode ser desligado com `cronos.worker.enabled=false` para ter instâncias só de scheduler).

```mermaid
stateDiagram-v2
    [*] --> PENDING: scheduler cria
    PENDING --> RUNNING: claim (SKIP LOCKED + lease)
    RUNNING --> SUCCEEDED: 2xx
    RUNNING --> PENDING: falha temporária, com backoff
    RUNNING --> DEAD: falha temporária na última tentativa
    RUNNING --> FAILED: 4xx ou payload inválido
    RUNNING --> PENDING: lease expirou (reaper)
    PENDING --> CANCELLED: POST /executions/{id}/cancel
    RUNNING --> CANCELLED: POST /executions/{id}/cancel
    DEAD --> PENDING: POST /executions/{id}/retry
    FAILED --> PENDING: POST /executions/{id}/retry
```

1. **Claim.** O worker reserva até o número de slots livres (`concurrency`, padrão 16) com um único `UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED)`. A execução vira `RUNNING`, recebe `locked_by` (id do worker), `lease_expires_at` e `attempt + 1`. Só reserva o que consegue executar agora, para não segurar trabalho que outro worker poderia pegar. Enquanto a fila tiver trabalho, volta a reservar assim que um slot vaga; quando ela esvazia, espera o próximo poll (500 ms).
2. **Webhook.** Cada execução roda numa virtual thread. O payload do job define a chamada:
   ```json
   { "url": "https://...", "method": "POST", "headers": { "Authorization": "..." }, "body": { ... } }
   ```
   `method` é `POST` por padrão. O worker envia os headers `Idempotency-Key` (id da execução), `X-Cronos-Execution-Id`, `X-Cronos-Job-Id`, `X-Cronos-Attempt` e `X-Cronos-Scheduled-For`. O `timeoutSeconds` do job é o prazo total da chamada.
3. **Resultado.** Gravado só se a execução ainda for deste worker (`WHERE locked_by = ? AND status = 'RUNNING'`). Se o lease expirou e o reaper já a devolveu para a fila, o resultado atrasado é descartado.

| Resposta | Tipo | Destino |
|---|---|---|
| 2xx | sucesso | `SUCCEEDED` |
| 5xx, 408, 429, timeout, erro de conexão | temporária | `PENDING` com backoff; `DEAD` ao atingir `maxAttempts` |
| demais 4xx, payload sem `url` ou inválido | definitiva | `FAILED`, sem retentativa |

**Backoff** exponencial com *equal jitter*: `base * 2^(tentativa-1)`, limitado a `backoff-max`, e sorteado entre metade e o valor cheio. Com o padrão de 1 s: ~0,5–1 s, ~1–2 s, ~2–4 s... até 5 min.

**Heartbeat e reaper.** Enquanto há chamadas em andamento, o worker renova o lease a cada 10 s (lease de 30 s). Se ele morrer ou travar, o lease vence e o reaper (em qualquer instância, a cada 5 s) devolve a execução para `PENDING`, ou `DEAD` se ela já usou todas as tentativas.

**Cancelamento.** `POST /executions/{id}/cancel` marca a execução como `CANCELLED`. Se ela estiver `RUNNING`, o worker que a detém percebe em até 1 s (`cancel-check-interval-ms`) e aborta a chamada HTTP. O cancelamento aborta só a requisição, sem interromper a thread: um interrupt numa virtual thread fecharia a conexão JDBC se chegasse no meio da gravação do resultado. Uma chamada que já chegou ao destino pode ter produzido efeito lá.

**Disparo manual e retentativa.** `POST /jobs/{id}/trigger` cria uma execução agora (`triggeredBy = MANUAL`), em qualquer status do job, sem alterar o `next_fire_at`, e respeita `allowOverlap`. `POST /executions/{id}/retry` recoloca uma execução `DEAD` ou `FAILED` na fila com as tentativas zeradas e **o mesmo id**: é a mesma ocorrência, então a mesma chave de idempotência.

**Desligamento.** O worker para de reservar e espera até `shutdown-grace` (30 s) pelas chamadas em andamento. O que não terminar é recuperado pelo reaper.

**Semântica de entrega: at-least-once.** Se o worker morrer depois de chamar o webhook e antes de gravar o resultado, a execução roda de novo. O destino deve deduplicar pelo `Idempotency-Key`.

## Métricas

Expostas em `/actuator/prometheus`. Os nomes abaixo estão no formato do Micrometer; o Prometheus troca `.` por `_` e acrescenta sufixos como `_seconds` e `_total`.

| Métrica | Tipo | O que mede |
|---|---|---|
| `cronos.scheduler.lag` | timer (p50/p95/p99) | Atraso entre o `next_fire_at` e o momento em que o scheduler processou o job |
| `cronos.execution.start.delay` | timer (p50/p95/p99) | Atraso entre o horário lógico (`scheduled_for`) e o início da 1ª tentativa: o atraso de disparo visto pelo destino |
| `cronos.scheduler.executions.created` | contador | Execuções criadas pelo scheduler |
| `cronos.scheduler.executions.duplicate` | contador | Ocorrências que outra instância já tinha criado (barradas pelo `UNIQUE`) |
| `cronos.scheduler.misfires{policy}` | contador | Misfires por política |
| `cronos.scheduler.overlap.skipped` | contador | Ocorrências ignoradas por `allowOverlap = false` |
| `cronos.scheduler.jobs.invalid` | contador | Jobs pausados por agendamento inválido |
| `cronos.worker.executions{outcome}` | contador | Resultado de cada tentativa: `succeeded`, `retry`, `dead`, `failed`, `discarded`, `error` |
| `cronos.webhook.duration{result}` | timer | Duração das chamadas ao webhook, por `success`/`failure` |
| `cronos.worker.inflight` | gauge | Execuções em andamento nesta instância |
| `cronos.worker.reaped` | contador | Execuções recuperadas de workers que pararam de responder |
| `cronos.retention.deleted` | contador | Execuções apagadas pela retenção |
| `cronos.security.rate.limited{reason}` | contador | Respostas `429`: `key` (limite da chave) ou `auth` (tentativas inválidas por IP) |

Taxa de falha, por exemplo: `sum(rate(cronos_worker_executions_total{outcome=~"retry|dead|failed"}[5m])) / sum(rate(cronos_worker_executions_total[5m]))`.

## Segurança

### Autenticação

Cada requisição precisa de uma API key em `Authorization: Bearer <chave>` ou `X-API-Key: <chave>`; sem ela, a resposta é `401` com `WWW-Authenticate: Bearer`. Só `/actuator/health` é público, para os probes de liveness e readiness.

- **Sem chave configurada, a aplicação não sobe.** Isso evita publicar por engano uma API aberta.
- **Várias chaves ao mesmo tempo**, para rotação: adicione a nova em `CRONOS_API_KEYS`, migre os clientes e remova a antiga.
- **Comparação em tempo constante** sobre o SHA-256 das chaves, para o tempo de resposta não revelar quanto da chave está certo.
- É um filtro de servlet simples, sem Spring Security: todas as chaves têm o mesmo acesso. Não há usuários, papéis nem escopos por job.

### Rate limit

O mesmo filtro aplica dois limites por *token bucket* ([`TokenBucketLimiter`](src/main/kotlin/io/github/juliofilizzola/cronos/security/TokenBucketLimiter.kt)). Cada cliente tem um balde de fichas que se recarrega numa vazão fixa, e cada requisição gasta uma: o tamanho do balde é a rajada tolerada, a recarga é a vazão sustentada.

| Limite | Por | Padrão | Para quê |
|---|---|---|---|
| Requisições autenticadas | chave | 200/s, rajada de 400 | Um cliente abusivo (ou com bug em loop) não derruba o banco para os outros |
| Tentativas sem chave válida | IP | 10/min, rajada de 10 | Frear força bruta de chaves |

- Acima do limite, a resposta é `429 Too Many Requests` com `Retry-After` (segundos) e corpo `ProblemDetail`. Respostas autenticadas trazem `X-RateLimit-Limit` e `X-RateLimit-Remaining`.
- **A chave é validada antes do limite por IP.** Só tentativas inválidas gastam do balde do IP, então quem tem chave válida não é bloqueado pelos erros de outro cliente atrás do mesmo NAT.
- **O IP é o da conexão.** `X-Forwarded-For` só é usado com `trust-forwarded-for: true`, porque sem um proxy que sobrescreva o header qualquer um trocaria de "IP" a cada tentativa.
- O número de clientes rastreados tem teto (`max-tracked-clients`); baldes ociosos são descartados primeiro, o que não muda nada, porque um balde novo também nasce cheio.
- `/actuator/health` não é limitado. A métrica `cronos.security.rate.limited{reason="key"|"auth"}` conta as respostas `429`.

**Limitação:** o estado fica na memória de cada instância. Com N instâncias atrás de um balanceador, o limite efetivo chega a N vezes o configurado. Um limite global exigiria guardar a contagem no PostgreSQL ou num Redis.

Validação contra a aplicação rodando (perfil `local`, limites padrão):

| Cenário | Resultado |
|---|---|
| 3000 requisições com chave válida, 100 em paralelo | 785 × `200` e 2215 × `429` em 2,3 s: rajada de 400 + ~200/s, com `Retry-After: 1` |
| ~240 req/s com chave válida por 6 s, partindo do balde cheio | 1500 × `200`: o balde absorve o excesso sobre 200/s |
| 50 chaves erradas do mesmo IP | 10 × `401` e depois 40 × `429` com `Retry-After: 6` |
| Requisições sem chave do mesmo IP, logo depois | todas `429` |
| Chave válida do IP bloqueado | `200` |
| `scripts/load.mjs` criando 1000 jobs | criação cai de ~1500/s para ~320/s, com 40 respostas `429` respeitadas; a vazão de execuções não muda (1039/s), porque os webhooks não passam pela API |

### SSRF

Sem proteção, qualquer um com acesso à API poderia criar um job apontando para `http://169.254.169.254/` (credenciais da nuvem), para o PostgreSQL ou para serviços internos, e o worker faria a chamada de dentro da rede. Por isso o [`UrlGuard`](src/main/kotlin/io/github/juliofilizzola/cronos/security/UrlGuard.kt):

- resolve o host e recusa se **qualquer** um dos endereços for interno: loopback, `0.0.0.0/8`, `10/8`, `172.16/12`, `192.168/16`, `100.64/10` (CGNAT), `169.254/16` (link-local e metadata da nuvem), multicast e reservados, e no IPv6 `::1`, `fc00::/7`, `fe80::/10` e IPv4 embutido (`::ffff:a.b.c.d`, NAT64);
- recusa credenciais na URL (`http://user:pass@...`) e, se configurado, portas fora de `allowed-ports`;
- roda **na criação do job** (`400`) e **de novo antes de cada chamada** do worker, porque o DNS do host pode ter mudado desde a criação. No worker, destino bloqueado vira `FAILED` sem retentativa; host que não resolve é falha temporária;
- não segue redirecionamentos, então um `302` para um endereço interno não é seguido.

Para liberar um serviço interno conhecido, use `cronos.webhook.allowed-hosts`. `allow-private-networks: true` desliga a checagem e só deve ser usado em desenvolvimento.

**Limitação:** a checagem acontece antes da conexão, e o `HttpClient` resolve o nome de novo ao conectar. Um DNS malicioso com TTL zero (*DNS rebinding*) ainda pode trocar o IP nesse intervalo. Para fechar isso de vez, faça a saída do worker passar por um proxy de egress que aplique a mesma regra, ou restrinja a rede no nível do container ou da VPC.

## Retenção do histórico

`DELETE /jobs/{id}` apaga o job e todas as execuções dele numa transação. A linha do job é travada antes, para que o scheduler (que usa `SKIP LOCKED`) não crie uma execução nova no meio. Se houver uma execução em andamento, o resultado dela é descartado pelo worker, porque a linha não existe mais.

A limpeza automática ([`ExecutionRetention`](src/main/kotlin/io/github/juliofilizzola/cronos/scheduling/ExecutionRetention.kt)) roda a cada hora em todas as instâncias e apaga execuções **terminadas** (`SUCCEEDED`, `FAILED`, `DEAD`, `CANCELLED`) com `finished_at` mais antigo que 30 dias. `PENDING` e `RUNNING` nunca são apagadas, nem os jobs.

- Apaga em lotes de 1000 até não sobrar nada vencido, para não segurar locks nem gerar uma transação enorme.
- Usa `FOR UPDATE SKIP LOCKED`: várias instâncias limpam ao mesmo tempo sem disputar linhas.
- A migration `V4` cria um índice parcial em `finished_at` só para os status terminados, então a busca não varre a fila.
- A métrica `cronos.retention.deleted` conta o que foi apagado.

## Stack

- Kotlin e Java 21
- Spring Boot 4 (Web, JDBC, Validation, Actuator)
- PostgreSQL 16
- Flyway para migrations
- Micrometer + Prometheus para métricas
- JUnit 5 nos testes
- Docker Compose para o ambiente local

## Como rodar

### Pré-requisitos

- JDK 21
- Docker

### Passos

```bash
# 1. Subir o PostgreSQL
docker compose up -d

# 2. Rodar a aplicação (o Flyway cria as tabelas na primeira execução)
./gradlew bootRun        # no Windows: .\gradlew.bat bootRun
```

A API fica em `http://localhost:8080`. O scheduler começa a rodar junto com a aplicação.

O `bootRun` ativa o perfil `local` ([`application-local.yaml`](src/main/resources/application-local.yaml)), que define a chave de API `dev-key` e libera webhooks em `localhost` para os scripts de teste. Fora dele (o jar, em produção), a aplicação **não sobe sem chave**:

```bash
CRONOS_API_KEYS=uma-chave-longa-e-aleatoria java -jar build/libs/Cronos-0.0.1-SNAPSHOT.jar
```

Para zerar o banco: `docker compose down -v`.

### Configuração

`src/main/resources/application.yaml`:

| Propriedade | Padrão | Descrição |
|---|---|---|
| `cronos.scheduler.batch-size` | `100` | Máximo de jobs processados por transação |
| `cronos.scheduler.poll-interval-ms` | `1000` | Intervalo entre ticks do scheduler |
| `cronos.worker.enabled` | `true` | Liga o worker nesta instância |
| `cronos.worker.concurrency` | `16` | Execuções simultâneas por instância |
| `cronos.worker.poll-interval-ms` | `500` | Intervalo entre tentativas de claim |
| `cronos.worker.lease` | `30s` | Validade da reserva sem heartbeat |
| `cronos.worker.heartbeat-interval-ms` | `10000` | Intervalo de renovação do lease |
| `cronos.worker.cancel-check-interval-ms` | `1000` | Intervalo para perceber cancelamentos de execuções em andamento |
| `cronos.worker.reaper-interval-ms` | `5000` | Intervalo do reaper |
| `cronos.worker.backoff-base` | `1s` | Espera antes da 2ª tentativa |
| `cronos.worker.backoff-max` | `5m` | Teto do backoff |
| `cronos.worker.shutdown-grace` | `30s` | Espera pelas execuções em andamento ao desligar |
| `cronos.security.enabled` | `true` | Exige API key |
| `cronos.security.api-keys` | `${CRONOS_API_KEYS}` | Chaves aceitas, separadas por vírgula. Obrigatório com a segurança ligada |
| `cronos.security.public-paths` | `/actuator/health` | Prefixos acessíveis sem chave |
| `cronos.security.rate-limit.enabled` | `true` | Liga o rate limit |
| `cronos.security.rate-limit.requests-per-second` | `200` | Vazão sustentada por chave |
| `cronos.security.rate-limit.burst` | `400` | Rajada tolerada por chave |
| `cronos.security.rate-limit.failed-auth-per-minute` | `10` | Tentativas sem chave válida aceitas por IP, por minuto |
| `cronos.security.rate-limit.failed-auth-burst` | `10` | Rajada de tentativas sem chave válida por IP |
| `cronos.security.rate-limit.trust-forwarded-for` | `false` | Usa `X-Forwarded-For` como IP (só atrás de proxy confiável) |
| `cronos.security.rate-limit.max-tracked-clients` | `10000` | Teto de chaves/IPs rastreados em memória |
| `cronos.webhook.allow-private-networks` | `false` | Libera destinos internos (só para desenvolvimento) |
| `cronos.webhook.allowed-hosts` | `[]` | Hosts liberados mesmo resolvendo para endereço interno |
| `cronos.webhook.allowed-ports` | `[]` | Portas aceitas nos webhooks (vazio = todas) |
| `cronos.retention.enabled` | `true` | Liga a limpeza do histórico |
| `cronos.retention.executions-max-age` | `30d` | Idade a partir da qual execuções terminadas são apagadas |
| `cronos.retention.interval-ms` | `3600000` | Intervalo entre limpezas |
| `cronos.retention.batch-size` | `1000` | Linhas apagadas por comando |

## API

O arquivo [`jobs.http`](jobs.http) tem exemplos prontos para o cliente HTTP do IntelliJ (ambiente `local`, definido em [`http-client.env.json`](http-client.env.json)).

Toda rota exige `Authorization: Bearer <chave>` (ou `X-API-Key: <chave>`), exceto `/actuator/health`. Veja [Segurança](#segurança).

| Método | Rota | Descrição |
|---|---|---|
| `POST` | `/jobs` | Cria um job |
| `GET` | `/jobs?status=&page=&size=` | Lista jobs (tamanho máximo de página: 100) |
| `GET` | `/jobs/{id}` | Detalhe do job |
| `GET` | `/jobs/{id}/executions` | Histórico de execuções |
| `POST` | `/jobs/{id}/pause` | Pausa um job `ACTIVE` |
| `POST` | `/jobs/{id}/resume` | Retoma um job `PAUSED` |
| `DELETE` | `/jobs/{id}` | Apaga o job e todo o histórico de execuções (`204`) |
| `POST` | `/jobs/{id}/trigger` | Dispara o job agora, fora do agendamento |
| `GET` | `/executions/{id}` | Detalhe de uma execução |
| `POST` | `/executions/{id}/cancel` | Cancela uma execução `PENDING` ou `RUNNING` |
| `POST` | `/executions/{id}/retry` | Recoloca na fila uma execução `DEAD` ou `FAILED` |
| `GET` | `/actuator/health` | Saúde da aplicação (pública) |
| `GET` | `/actuator/prometheus` | Métricas (exige chave; configure o scrape com `authorization.credentials`) |

### Criar um job

```bash
curl -X POST http://localhost:8080/jobs \
  -H "Authorization: Bearer dev-key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "relatorio-horario",
    "scheduleType": "CRON",
    "scheduleExpr": "0 0 * * * *",
    "timezone": "America/Sao_Paulo",
    "misfirePolicy": "SKIP",
    "payload": { "url": "https://example.com/relatorio", "method": "POST" }
  }'
```

### Payload

| Campo | Obrigatório | Descrição |
|---|---|---|
| `url` | sim | URL `http` ou `https` chamada pelo worker |
| `method` | não | `GET`, `POST` (padrão), `PUT`, `PATCH` ou `DELETE` |
| `headers` | não | Objeto de strings, enviado como headers |
| `body` | não | Qualquer JSON, enviado como corpo com `Content-Type: application/json` |

O payload é validado na criação do job (`400` se inválido), incluindo a [proteção contra SSRF](#ssrf).

### Formatos de agendamento

| `scheduleType` | `scheduleExpr` | Exemplo |
|---|---|---|
| `CRON` | 6 campos, **com segundos** (formato do Spring) | `0 */5 * * * *` (a cada 5 minutos) |
| `INTERVAL` | Duração ISO-8601, mínimo de 1 segundo | `PT30S`, `PT5M`, `PT2H` |
| `ONCE` | Instante ISO-8601 no futuro | `2026-12-31T23:00:00Z` |

### Campos opcionais

| Campo | Padrão | Descrição |
|---|---|---|
| `timezone` | `UTC` | Fuso usado para avaliar o cron |
| `misfirePolicy` | `FIRE_ONCE` | `SKIP`, `FIRE_ONCE` ou `FIRE_ALL` |
| `misfireThresholdSeconds` | `60` | Tolerância antes de considerar misfire |
| `allowOverlap` | `false` | Permite nova execução com outra ainda ativa |
| `maxAttempts` | `3` | Tentativas máximas (1 a 20) antes de virar `DEAD` |
| `timeoutSeconds` | `60` | Prazo total de cada chamada ao webhook |

### Erros

As respostas de erro seguem o formato `ProblemDetail` (RFC 7807): `401` sem chave ou com chave inválida, `400` para agendamento, payload ou campos inválidos, `404` para job ou execução inexistente e `409` para transição de status inválida (por exemplo, pausar um job já pausado, cancelar uma execução já concluída ou disparar manualmente um job sem sobreposição que já tem execução ativa).

## Decisões de projeto

**PostgreSQL como fila, sem Redis nem Kafka.** `FOR UPDATE SKIP LOCKED` permite que vários consumidores disputem linhas sem bloqueio mútuo. Isso reduz a infraestrutura a um único componente e dá consistência transacional entre definição do job, execução e histórico. O custo é um teto de vazão menor que o de um broker dedicado, o que é aceitável para o volume esperado de um agendador.

**`scheduled_for` é o horário lógico, não o horário real do disparo.** A chave `UNIQUE (job_id, scheduled_for)` identifica "a ocorrência das 10:00 do job X", independentemente de quando ela foi criada. É isso que torna a deduplicação confiável mesmo com atrasos e reprocessamentos.

**A decisão de misfire é uma função pura.** O `MisfireResolver` recebe o job e o horário atual e devolve o que fazer, sem acessar banco nem relógio. Isso permite testar todas as políticas com testes unitários rápidos, sem containers.

**`resume` não recalcula `next_fire_at`.** Se o horário passou durante a pausa, a política de misfire do job decide o que fazer com o tempo perdido. O comportamento fica consistente entre "ficou parado por falha" e "ficou parado por pausa".

**`JdbcTemplate` em vez de JPA.** As consultas centrais (`SKIP LOCKED`, `ON CONFLICT`, updates condicionais) exigem SQL explícito, e o ORM atrapalharia mais do que ajudaria.

**Cron do Spring (`CronExpression`).** Já vem no framework, aceita segundos e é avaliado sobre `ZonedDateTime`, então fuso e horário de verão são tratados pelo `java.time`.

**Relógio injetado (`Clock`).** Facilita testes e deixa explícito de onde vem o "agora".

## Estrutura do projeto

```
cronos/
├── docker-compose.yaml
├── jobs.http
├── http-client.env.json      # chave de API do ambiente "local" para o jobs.http
├── scripts/
│   ├── e2e-workers.mjs     # teste ponta a ponta com receptor de webhooks
│   ├── chaos.mjs           # mata instâncias aleatoriamente e verifica duplicidade e perda
│   └── load.mjs            # mede vazão e atraso de disparo
└── src/
    ├── main/
    │   ├── kotlin/io/github/juliofilizzola/cronos/
    │   │   ├── api/           # controllers, DTO de entrada, tratamento de erros
    │   │   ├── service/       # regras de negócio, validação de agendamento e de payload
    │   │   ├── persistence/   # repositórios com SQL explícito (jobs e executions)
    │   │   ├── scheduling/    # SchedulerLoop, MisfireResolver, NextFireCalculator, ExecutionRetention
    │   │   ├── worker/        # Worker, WebhookClient, RetryPolicy, WorkerProperties
    │   │   ├── security/      # ApiKeyFilter, UrlGuard (SSRF)
    │   │   ├── domain/        # Job, JobDetails, enums, exceções
    │   │   └── config/        # beans (Clock, calculadora, resolver, retry, cliente HTTP, filtro de API key)
    │   └── resources/
    │       ├── application.yaml
    │       └── db/migration/  # V1__init, V2__misfire_and_completed, V3__manual_trigger_and_cancel, V4__retention_index
    └── test/kotlin/...        # testes unitários e de integração (Testcontainers)
```

## Testes

Com o Docker no ar (os testes de integração sobem um PostgreSQL 16 via Testcontainers, compartilhado por todas as classes):

```bash
./gradlew test
```

Os testes unitários não dependem de banco e podem rodar isolados:

```bash
./gradlew test --tests '*MisfireResolverTest' --tests '*RetryPolicyTest'
```

- `MisfireResolverTest`: disparo dentro da tolerância, cada uma das três políticas, o limite do `FIRE_ALL`, cron com a próxima ocorrência futura e job `ONCE` perdido.
- `ApiKeyFilterTest`: 401 sem chave ou com chave errada, `Bearer` e `X-API-Key`, várias chaves, rotas públicas por prefixo exato e falha ao subir sem chave.
- `TokenBucketLimiterTest`: rajada e bloqueio, recarga na vazão configurada sem passar da capacidade, clientes independentes, 1000 requisições concorrentes liberando exatamente a capacidade e teto de memória.
- `ApiKeyFilterRateLimitTest`: 429 com `Retry-After` e headers `X-RateLimit-*`, balde por chave, força bruta passando de 401 para 429, chave válida não bloqueada pelos erros de outro cliente no mesmo IP, `X-Forwarded-For` ignorado salvo configuração e limite desligado.
- `UrlGuardTest`: bloqueio de cada faixa interna (IPv4, IPv6 e IPv4 embutido), host com um IP público e um privado, DNS que não resolve, credenciais na URL, portas e as liberações por configuração.
- `RetryPolicyTest`: sucesso, retentativa, `DEAD` na última tentativa, falha definitiva, crescimento e teto do backoff e classificação dos status HTTP.

Testes de integração:

- `ConcurrencyIntegrationTest`: 8 threads disputando o claim de 300 execuções sem nenhuma reservada duas vezes; resultado de worker que perdeu a reserva descartado; reaper devolvendo para a fila ou marcando `DEAD`; heartbeat protegendo execução viva; 4 schedulers simultâneos recuperando 30 s perdidos de 20 jobs sem duplicar nem deixar buracos; transições de disparo manual, cancelamento e retentativa.
- `MaintenanceIntegrationTest`: `DELETE` apagando job e histórico; retenção apagando só execuções terminadas e antigas, em lotes, sem tocar em `PENDING`/`RUNNING` nem no job.
- `SsrfIntegrationTest`: com a configuração padrão, a criação recusa `localhost`, metadata da nuvem, redes privadas e `::1`, e aceita um IP público.
- `WorkerIntegrationTest`: worker real contra um servidor HTTP local: sucesso com `Idempotency-Key`, 500 retentado até `DEAD`, 404 sem retentativa, timeout, cancelamento interrompendo uma chamada em andamento e retentativa de `DEAD` com o mesmo id.

### Teste ponta a ponta

Com a aplicação rodando, o script [`scripts/e2e-workers.mjs`](scripts/e2e-workers.mjs) (Node 22, sem dependências) sobe um receptor de webhooks na porta 4000, cria sete jobs (sucesso, sem sobreposição, falha 500, webhook lento, disparo manual, cancelamento e `ONCE`), mostra cada transição de status e cada chamada recebida em tempo real e, no fim, roda 16 verificações: API exige chave, `DELETE` apaga job e histórico, nada duplicado, entrega única, `Idempotency-Key`, sobreposição, retentativas, `DEAD`, timeout, disparo manual, cancelamento de execução em andamento, retentativa de `DEAD` via API e `ONCE`.

```bash
node scripts/e2e-workers.mjs
```

Variáveis: `CRONOS_URL` (padrão `http://localhost:8080`), `CRONOS_API_KEY` (`dev-key`), `WEBHOOK_PORT` (`4000`), `WEBHOOK_HOST` (`localhost`; use `host.docker.internal` se o Cronos rodar em container) e `DURATION_S` (`30`). O Cronos precisa aceitar webhooks em `localhost`, o que o perfil `local` já faz. Sai com código 0 se tudo passar. Os jobs criados são apagados no final.

### Teste de caos

[`scripts/chaos.mjs`](scripts/chaos.mjs) sobe 3 instâncias do jar sobre o mesmo PostgreSQL, cria 20 jobs que disparam a cada segundo (`FIRE_ALL`, para que todo segundo perdido seja recuperado) e, durante 60 s, mata uma instância aleatória a cada 5 s com `SIGKILL` (sem desligamento limpo), reiniciando-a de 2 a 6 s depois. No fim, pausa os jobs, espera a fila esvaziar e verifica: nenhuma ocorrência duplicada, nenhum segundo faltando em nenhum job, nada preso em `PENDING`/`RUNNING`, tudo `SUCCEEDED` e toda execução entregue ao webhook.

```bash
./gradlew bootJar
node scripts/chaos.mjs
```

Resultado de uma execução: 12 mortes, 1438 execuções, 0 duplicadas, 0 perdidas, 24 execuções que precisaram de mais de uma tentativa (estavam em voo num worker morto) e 6 entregas repetidas ao webhook, o esperado com *at-least-once*. Variáveis: `INSTANCES`, `JOBS`, `DURATION_S`, `KILL_EVERY_S`, `DRAIN_S`, `BASE_PORT`, `WEBHOOK_PORT` e `JAVA`. Os logs das instâncias ficam em `build/chaos-logs/`.

## Desempenho

[`scripts/load.mjs`](scripts/load.mjs) cria N jobs disparando a cada segundo e mede, no receptor de webhooks, as execuções entregues por segundo e o atraso entre o horário lógico (`X-Cronos-Scheduled-For`) e a chegada da chamada. Cada degrau tem 10 s de aquecimento e 30 s de medição.

```bash
node scripts/load.mjs
```

Uma instância com a configuração padrão (`concurrency: 16`, pool de 10 conexões), PostgreSQL 16 no Docker Desktop, tudo na mesma máquina (AMD Ryzen 7 5700G, 16 threads, 24 GB, Windows 11):

| jobs | esperado/s | entregue/s | atraso p50 | atraso p95 | atraso p99 | atraso máx |
|---|---|---|---|---|---|---|
| 100 | 100 | 97 | 969 ms | 3100 ms | 3749 ms | 3956 ms |
| 250 | 250 | 251 | 878 ms | 1367 ms | 1448 ms | 1504 ms |
| 500 | 500 | 514 | 877 ms | 1439 ms | 1472 ms | 1485 ms |
| 1000 | 1000 | 1005 | 1157 ms | 1512 ms | 1560 ms | 1623 ms |
| 2000 | 2000 | 1268 | 10974 ms | 15323 ms | 15699 ms | 15790 ms |

Com `cronos.worker.concurrency=64` e `spring.datasource.hikari.maximum-pool-size=40`, a mesma instância entrega os 2000/s com atraso p50 de 967 ms e p99 de 1463 ms.

Como ler:

- **O atraso de ~1 s com pouca carga é o intervalo de polling**, não processamento: o scheduler roda a cada 1 s e o worker a cada 500 ms, então uma ocorrência espera em média ~0,5 s por cada um. Para disparos mais pontuais, reduza `cronos.scheduler.poll-interval-ms`. O p99 alto do primeiro degrau vem do aquecimento da JVM.
- **Até 1000 execuções/s por instância** com a configuração padrão. Acima disso, o gargalo é o worker (16 slots, cada execução faz uma escrita própria): no degrau de 2000, `cronos.scheduler.lag` ficou com p50 de 0,5 s e `cronos.execution.start.delay` com p50 de 10,7 s. Mais concorrência e conexões, ou mais instâncias, resolvem.
- **Agrupar as escritas do scheduler fez diferença.** Antes do batch, o scheduler fazia duas idas ao banco por job e entregava só 724/s com 1000 jobs (atraso p50 de 7,6 s).
- A tabela foi medida antes do rate limit. Com ele, a **criação** de jobs pelo script fica limitada a 200/s por chave (o script espera o `Retry-After`); a vazão de **execuções** não muda, porque os webhooks não passam pela API.
- Os números são de uma máquina de desenvolvimento com banco, aplicação e gerador de carga juntos. Servem para comparar mudanças, não como capacidade de produção.

## Roadmap

- [x] Modelo de dados e migrations
- [x] Scheduler com `SKIP LOCKED` e deduplicação por `UNIQUE`
- [x] Política de misfire (`SKIP`, `FIRE_ONCE`, `FIRE_ALL`)
- [x] API de jobs (criar, consultar, listar, pausar, retomar) e histórico de execuções
- [x] Worker: claim de execuções com lease e execução de webhooks
- [x] Heartbeat e reaper para recuperar execuções de workers que morreram
- [x] Retentativas com backoff exponencial e jitter, e dead-letter (`DEAD`)
- [x] Timeout de execuções
- [x] Teste ponta a ponta com receptor de webhooks (`scripts/e2e-workers.mjs`)
- [x] Cancelamento de execuções em andamento
- [x] Endpoint de disparo manual e retentativa de execução `DEAD`
- [x] Validação do payload na criação do job
- [x] Métricas customizadas (atraso de disparo, taxa de falha, misfires)
- [x] Testes de integração com Testcontainers
- [x] Teste de caos: matar schedulers e workers aleatoriamente e verificar que nada duplica nem se perde
- [x] Teste de carga com números no README
- [x] Autenticação por API key
- [x] Rate limit por chave e por IP (tentativas inválidas)
- [x] Proteção contra SSRF nos webhooks
- [x] `DELETE` de jobs e retenção automática do histórico

## Limitações conhecidas

- **Autenticação só por API key**, sem usuários, papéis ou escopos: quem tem uma chave pode tudo.
- **Rate limit por instância:** com N instâncias, o limite efetivo chega a N vezes o configurado. Veja [Rate limit](#rate-limit).
- **Jobs criados antes da validação de payload** podem ter payload inválido; cada execução deles termina em `FAILED`.
- **Jobs `COMPLETED` não são apagados automaticamente**, só as execuções antigas. Apague-os com `DELETE /jobs/{id}`.
- **Cancelar não desfaz efeitos:** se a chamada já chegou ao destino, ele pode ter agido.
- **SSRF por DNS rebinding:** o `UrlGuard` checa o IP antes da conexão, mas um DNS malicioso pode trocá-lo no intervalo. Veja [SSRF](#ssrf).
- **Dependência de relógio:** o scheduler usa o relógio da JVM. Com instâncias com horários muito diferentes, os disparos podem variar levemente. Usar o `now()` do PostgreSQL é uma alternativa avaliada.
- **Garantia de entrega:** *at-least-once*, o que exige idempotência do lado de quem recebe. O `id` da execução vai no header `Idempotency-Key`.

## Autor

[@juliofilizzola](https://github.com/juliofilizzola)