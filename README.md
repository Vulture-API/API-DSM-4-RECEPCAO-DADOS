# Station Ingest

Recepção Python de telemetria: **MQTT → Redis Stream**. Valida apenas o envelope;
validação de negócio, relações e persistência definitiva pertencem ao serviço
independente `API-DSM-4-PERSISTENCIA-DADOS`.

[Contrato oficial e evolução no Confluence](https://vultureapi.atlassian.net/wiki/spaces/V/pages/46333954).

## Contrato

Assinatura padrão `estacoes/+/dados`, publicação `estacoes/<MAC>/dados` e Stream
`telemetry:ingest`. O payload é objeto JSON UTF-8, com `estacao_id` string não
vazia e até `INGEST_MAX_PAYLOAD_BYTES` (262144 por padrão). Não consulta cadastros
nem PostgreSQL. JSON inválido, raiz não objeto, identificação inválida e tamanho
excessivo são rejeitados, contabilizados por motivo e confirmados no MQTT.

Cada entrada mantém os cinco campos:

| Campo | Conteúdo |
| --- | --- |
| `estacao_id` | Identificação extraída, sem espaços externos |
| `unix_time` | Inteiro não negativo em segundos Unix, como texto; nos demais casos, texto `null` |
| `received_at` | Milissegundos Unix capturados na entrada; estável nas retentativas locais |
| `topic` | Tópico original |
| `payload` | JSON UTF-8 original, incluindo tipos, valores e campos adicionais |

```json
{"estacao_id":"00:1A:2B:3C:4D:01","unix_time":"inválido","temp":23.7,"umid":64,"extra":{"versao":2}}
```

Nesse exemplo, o `unix_time` extraído é `"null"`, mas o payload mantém
`"inválido"`. Ausência de `unix_time`, `null` explícito e valores inválidos são
distintos no payload. Eventos antigos já normalizados não permitem reconstruir
o valor original; o tratamento deve identificar essa limitação e usar o instante
de recepção conforme o contrato. A recepção não interpreta as medições.

## Execução

Python 3.12+, broker MQTT e Redis acessíveis:

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e '.[test,lint]'
cp .env.example .env
python -m station_ingest
```

Configurações `INGEST_` estão em `.env.example`. Autenticação MQTT, TLS com CA e
certificado/chave e URLs Redis `redis://`/`rediss://` continuam disponíveis.
O processo de recepção funciona sem `INGEST_DATABASE_URL`.

## Confirmação, filas e falhas

Usa Paho MQTT 2.x, callbacks versão 2, MQTT 3.1.1, sessão persistente, client ID
configurável e assinatura QoS 1. [API pública de confirmação manual](https://eclipse.dev/paho/files/paho.mqtt.python/html/client.html#paho.mqtt.client.Client.ack).
Uma mensagem válida só é confirmada após resposta bem-sucedida de seu `XADD`.
Falha ou resultado ambíguo provoca retentativa e pode produzir duplicata.

A ponte da thread MQTT para `asyncio` e a fila do gravador têm capacidades
limitadas. Saturação fecha a conexão sem confirmar mensagens ainda não gravadas;
a reconexão usa o mesmo client ID e sessão. Confirmações de conexões anteriores
são invalidadas. No encerramento a admissão para, o gravador drena dentro do prazo
e a conexão fecha; trabalho não concluído permanece sem confirmação.

A garantia depende de publicação QoS 1 e da disponibilidade/durabilidade do
broker e Redis. QoS 0 não tem proteção equivalente. Não há entrega exatamente
uma vez, deduplicação ou garantia contra perda de armazenamento do broker/Redis.
`received_at` é preservado durante retentativas locais; redelivery MQTT após
reconexão recebe um novo instante de recepção, pois o protocolo atual não inclui
uma identidade durável de evento. O tratamento deve considerar essa limitação.

Não execute duas instâncias com o mesmo `INGEST_MQTT_CLIENT_ID`. Para particionar
consumo, defina tópicos e client IDs próprios; assinatura comum em duas instâncias
pode duplicar o recebimento.

Logs `ingest_metrics`: recebidos, gravados, rejeitados por motivo, retentativas,
reconexões, saturações, ACKs, profundidades da ponte/fila, taxa gravada e latência
média/máxima da entrada até a resposta Redis. Não despejam payload completo.

## Retenção segura — SCRUM-530

`INGEST_RETENTION_ENABLED=false` continua sendo o padrão. Quando ativada, a
recepção executa um único script Lua atômico Redis 7: `TIME`, `XINFO GROUPS`,
`XPENDING` de **todos** os grupos e `XTRIM MINID ~ ... LIMIT`. O limite é o menor
entre idade configurada, último ID entregue por grupo e menor ID pendente por
grupo. O limite é exclusivo: a própria entrada de fronteira permanece. A
aproximação pode conservar entradas adicionais. Não existe fallback por idade,
MAXLEN ou uso de recursos exclusivos do Redis 8. IDs são comparados por componentes
decimais textuais, inclusive acima de 2^53.

Configuração (listas no ambiente usam JSON):

```dotenv
INGEST_RETENTION_ENABLED=false
INGEST_RETENTION_DAYS=7
INGEST_RETENTION_INTERVAL_SECONDS=60
INGEST_RETENTION_REQUIRED_GROUPS=["postgres-persister"]
INGEST_RETENTION_TRIM_LIMIT=10000
INGEST_RETENTION_MAX_GROUPS=128
```

Sem grupos, sem um grupo obrigatório, com metadados inválidos, acima do limite de
grupos ou com erro Redis, nenhuma limpeza acontece. Um grupo novo em `0-0` bloqueia
a limpeza; um grupo abandonado com PEL também. Configure todos os grupos essenciais
antes de ativar a política. Criar um grupo posteriormente não restaura histórico
já removido. Não use `XGROUP SETID`, `DELCONSUMER`, XDEL, MAXLEN ou expurgo manual
para esconder atrasos. A proteção depende de consumidores que só confirmam após
destino durável. A execução do consumo pertence ao tratamento independente.

`redis_retention_status` informa `removed`, `boundary`, `reason` e
`blocking_group`, mesmo sem remoções. `redis_retention_failed` registra o tipo de
falha. Alertar se a fronteira parar enquanto memória/backlog crescem; investigar
o grupo/PEL indicado e recuperar o tratamento. Nunca contornar o bloqueio com
corte só por idade. Aumentar capacidade ou pausar publicadores conforme o runbook
do ambiente. Redis deve ter persistência dimensionada e política **noeviction**;
essa rotina não impede perdas por evicção, perda de disco ou exclusão externa.
Monitore memória/disco, tamanho do Stream e indisponibilidade prolongada.

A limpeza permanece neste serviço; o tratamento não executa XTRIM. Não foi
ativada no Compose nesta entrega. Durabilidade do MQTT/Redis depende da
configuração operacional; os testes de redelivery mantêm o broker disponível,
e `persistence false` não protege reinício do broker.

## Tratamento independente

A recepção executa somente MQTT → Redis, sem cliente ou configuração PostgreSQL.
O serviço [API-DSM-4-PERSISTENCIA-DADOS](https://github.com/Vulture-API/API-DSM-4-PERSISTENCIA-DADOS)
executa `station-persist consume` com sua própria imagem e configuração `PERSIST_*`.
Ele realiza associação, validação, leituras, ocorrências, quarentena e recuperação.

O comando padrão e `station-ingest ingest` (também `python -m station_ingest ingest`)
continuam disponíveis. `persist` foi removido na **SCRUM-532** e encerra com erro e
orientação, sem iniciar conexões nem redirecionar para outro serviço.

## Verificação

```bash
ruff check .
ruff format --check .
RUN_INTEGRATION=1 \
TEST_MQTT_HOST=127.0.0.1 TEST_MQTT_PORT=1883 \
TEST_REDIS_URL=redis://127.0.0.1:6379/15 \
pytest --cov --cov-report=term-missing --junitxml=reports/junit.xml
python -m build
docker build -t station-ingest:local .
```

Use **Mosquitto e Redis descartáveis**, sem banco ou credenciais PostgreSQL.
Os testes usam identificadores aleatórios.
Sem `RUN_INTEGRATION=1`, integrações ficam ignoradas no desenvolvimento local.
O CI exige todas as integrações sem skips, cobertura mínima de 80%, pacote e build
Docker sem usuário root. Inclui queda real do receptor antes do XADD e após XADD
antes do ACK, aceitando duplicatas no segundo caso.

A imagem executa `python -m station_ingest` como usuário `ingest`:

```bash
docker run --rm --env-file .env station-ingest:local
```

## CI e análise de segurança

O workflow `.github/workflows/ci.yml` mantém os checks obrigatórios
**Lint, testes e build** e **Validar build da imagem Docker**. Executa lint e
formatação com Ruff fixado, testes unitários e integrações descartáveis, nenhum
skip permitido, cobertura mínima de 80%, pacote e imagem sem root. Os relatórios
ficam nos artefatos da execução; o resultado é registrado no PR pelo workflow.
Os testes de redelivery e retenção segura são obrigatórios. O CI também verifica
que o pacote e a imagem não incluem `asyncpg` nem o módulo de persistência antigo.

O workflow `.github/workflows/codeql.yml` preserva a análise Python de qualidade
e segurança adicionada na SCRUM-377, em PRs, dev/main e na agenda configurada.

## Medição de taxa

O publicador de teste requer o extra `test` (aiomqtt apenas para produzir mensagens):

```bash
python scripts/benchmark_publish.py --host localhost --messages 10000 --workers 10
```

Compare a taxa publicada com a taxa gravada, filas e latência dos logs
`ingest_metrics`. Registre versões, carga e configurações do ambiente. Nenhuma
meta de desempenho é presumida por esta entrega.
