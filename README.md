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

## Retenção

`INGEST_RETENTION_ENABLED=false` por padrão: nenhum corte automático por idade.
A limpeza que protege backlog e pendências será implementada na **SCRUM-530**.
O modo legado de `XTRIM MINID ~` por idade pode ser ativado explicitamente, mas é
inseguro para eventos ainda não processados e registra aviso ao iniciar.

Dimensione memória/disco, configure persistência Redis e broker e monitore atraso,
evicções e disponibilidade. Os testes de redelivery mantêm o broker disponível;
`persistence false` não garante recuperação após reinício dele.

## Persistidor legado

`python -m station_ingest persist` continua disponível, isolado em outro processo,
com `asyncpg`, configuração e testes preservados até **SCRUM-532**. Exige
`INGEST_DATABASE_URL` e usa as variáveis `INGEST_PERSIST_*`. Ainda contém as
limitações de associação, descarte, idempotência e recuperação descritas no
contrato. Sua retirada depende da substituição operacional em **SCRUM-531**;
o novo modo de inspeção não o substitui como gravador.

## Verificação

```bash
ruff check .
ruff format --check .
RUN_INTEGRATION=1 \
TEST_MQTT_HOST=127.0.0.1 TEST_MQTT_PORT=1883 \
TEST_REDIS_URL=redis://127.0.0.1:6379/15 \
TEST_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5432/telemetry_test \
pytest --cov --cov-report=term-missing --junitxml=reports/junit.xml
python -m build
docker build -t station-ingest:local .
```

Use **Mosquitto, Redis e PostgreSQL descartáveis**. Os testes usam identificadores
aleatórios; o teste PostgreSQL legado cria schema próprio e o remove ao final.
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
Os testes de redelivery e a suíte do persistidor legado também são obrigatórios.

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
