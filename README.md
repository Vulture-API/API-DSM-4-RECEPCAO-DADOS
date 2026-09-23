# Station Ingest

Serviço assíncrono para receber telemetria via MQTT, validar o envelope com
Pydantic e adicionar os eventos a um Redis Stream. O Redis é um buffer de sete
dias; a persistência histórica deve ser feita por outro consumidor.

## Contrato da mensagem

O payload MQTT deve ser um objeto JSON. `estacao_id` é obrigatório e deve ser
uma string não vazia. `unix_time` representa segundos Unix e é opcional: se
estiver ausente, negativo ou com tipo inválido, será armazenado como `null`.
Quaisquer outros campos são preservados.

```json
{
  "estacao_id": "estacao-42",
  "unix_time": 1760000000,
  "temperatura": 23.7,
  "umidade": 64
}
```

Cada entrada em `telemetry:ingest` contém os campos `estacao_id`, `unix_time`,
`received_at` (milissegundos Unix), `topic` e `payload` (JSON normalizado).
Duplicatas não são removidas.

Mensagens com JSON inválido, raiz que não seja objeto, `estacao_id` ausente ou
inválido, ou tamanho acima de `INGEST_MAX_PAYLOAD_BYTES` são descartadas e
contabilizadas nos logs.

## Execução local

Requer Python 3.12+, um broker MQTT e Redis acessíveis.

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e '.[test]'
cp .env.example .env
python -m station_ingest
```

Todas as configurações usam o prefixo `INGEST_`; consulte `.env.example` para
a lista completa. URLs `redis://` e `rediss://` são aceitas. Para TLS MQTT,
ative `INGEST_MQTT_TLS` e, quando necessário, informe CA e o par certificado /
chave do cliente.

As filas do serviço e do cliente MQTT têm limites configuráveis para proteger
a memória durante indisponibilidades. Como o `aiomqtt` 2.x confirma QoS 1
automaticamente, o broker não consegue aplicar backpressure fim a fim depois
desse ACK; monitore a profundidade da fila e dimensione os limites para a maior
indisponibilidade aceitável.

O serviço usa MQTT 3.1.1, QoS 1, sessão persistente e um client ID fixo. Não
execute duas instâncias com o mesmo `INGEST_MQTT_CLIENT_ID`: o broker derrubará
uma delas. Para escalar horizontalmente, particione tópicos e atribua um client
ID exclusivo por partição.

## Docker

```bash
docker build -t station-ingest:latest .
docker run --rm --env-file .env station-ingest:latest
```

A imagem contém apenas o serviço. Broker MQTT e Redis são externos.

## Retenção e operação do Redis

O serviço executa periodicamente `XTRIM MINID ~` usando o relógio do Redis. O
limite padrão é sete dias e é aproximado para evitar o custo de cortes exatos.

Para o ambiente de produção:

- habilite AOF com política de `fsync` compatível com a tolerância de perda;
- use replicação e monitore failover, memória e evicções;
- alerte sobre consumo de memória e atraso do grupo consumidor;
- dimensione a instância para sete dias no pico, incluindo overhead do Stream;
- não configure política de eviction que remova o Stream silenciosamente.

O `aiomqtt` 2.5 confirma mensagens QoS 1 antes de o código de aplicação gravar
no Redis. Sessão persistente protege períodos desconectados, mas permanece uma
janela de perda se o processo cair entre esse ACK e o `XADD`. Duplicatas também
podem ocorrer e o consumidor posterior deve ser idempotente.

## Testes

```bash
pytest
```

O teste de integração é desativado por padrão. Para executá-lo contra serviços
reais:

```bash
RUN_INTEGRATION=1 \
TEST_MQTT_HOST=localhost \
TEST_MQTT_PORT=1883 \
TEST_REDIS_URL=redis://localhost:6379/15 \
pytest -m integration
```

O teste cria chaves e tópicos com nomes aleatórios e remove o Stream ao final.

## Benchmark

Execute contra o ambiente de staging e acompanhe simultaneamente os logs de
`ingest_metrics` do serviço:

```bash
python scripts/benchmark_publish.py \
  --host mqtt.example.com \
  --messages 100000 \
  --workers 50
```

O script informa a taxa de publicação; a taxa persistida, profundidade da fila,
retentativas e descartes aparecem nos logs estruturados do serviço. O objetivo
de 10 mil mensagens/s depende da latência e do dimensionamento do broker, rede
e Redis e deve ser validado no ambiente de implantação.
