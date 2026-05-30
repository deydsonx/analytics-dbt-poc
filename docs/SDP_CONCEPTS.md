# Lakeflow Spark Declarative Pipelines (SDP) — Conceitos

> **Fonte:** https://docs.databricks.com/aws/en/ldp/concepts
> **Hub oficial:** https://docs.databricks.com/aws/en/ldp/
> **Compilado em:** 2026-05-18 — para o projeto `analytics-btc`

SDP = Lakeflow Spark Declarative Pipelines = LDP = SDP (todos sinônimos).
Substitui a API legada `dlt` (Delta Live Tables) — o módulo moderno é `pyspark.pipelines`.

---

## 1. O que é

> *"Lakeflow Spark Declarative Pipelines is a declarative framework for developing and running batch and streaming data pipelines in SQL and Python."*

Framework declarativo sobre Apache Spark que roda em Databricks Runtime, com compatibilidade DataFrame API. Você descreve **o que** as tabelas devem conter; o motor cuida de **como** processá-las (ordem, paralelismo, retries, checkpointing).

### Casos de uso principais
- Ingestão incremental de cloud storage (S3, ADLS Gen2, GCS) e message buses (Kafka, Kinesis, Pub/Sub, EventHub, Pulsar)
- Transformações batch e streaming incrementais (stateless e stateful)
- Stream processing em tempo real entre stores transacionais e bancos

### Benefícios
- **Orquestração automática** — ordem de execução + paralelismo derivados do grafo de dependências
- **Retries** — automáticos do nível Spark task → flow → pipeline
- **AUTO CDC API** — simplifica eventos de Change Data Capture com suporte a SCD Type 1 e 2; elimina manejo manual de watermark
- **Processamento incremental** — materialized views processam só dados novos/alterados

---

## 2. Componentes-chave

### 2.1 Flows
> *"Foundational data processing concept in SDP which supports both streaming and batch semantics."*

Tipos de flow:

| Tipo | Modo | Descrição |
|---|---|---|
| **Append** | Streaming | Único flow padrão exposto atualmente |
| Update | Streaming | Suportado, não exposto |
| Complete | Streaming | Suportado, não exposto |
| **AUTO CDC** | Streaming | Exclusivo SDP — lida com eventos CDC fora de ordem; SCD Type 1 e 2 |
| **Materialized View** | Batch | Processa só dados novos/alterados |

Função: ler de uma fonte, aplicar lógica, escrever em um target.

### 2.2 Streaming Tables
> *"A streaming table is a form of Unity Catalog managed table that is also a streaming target for Lakeflow SDP."*

- Pode receber 1+ streaming flows (Append, AUTO CDC)
- AUTO CDC é flow exclusivo de streaming tables
- Flows podem ser definidos explícitos/separados ou implícitos na definição da tabela
- **Quando usar:** janelas (tumbling/sliding/session), agregações incrementais, CDC

### 2.3 Materialized Views
> *"A materialized view is also a form of Unity Catalog managed table and is a batch target."*

- Pode receber 1+ MV flows (sempre definidos implicitamente na própria MV)
- **Quando usar:** agregações de tabela inteira (totais, médias), refresh batch

### 2.4 Sinks
> *"A sink is a streaming target for a pipeline and supports Delta tables, Apache Kafka topics, Azure EventHubs topics, and custom Python data sources."*

- Permite exportar stream pra serviços externos
- Recebe 1+ Append flows
- **Limitações:** apenas Python, apenas streaming, apenas append

### 2.5 Pipelines
> *"A pipeline is the unit of development and execution in Lakeflow SDP."*

Container que agrupa flows, streaming tables, MVs e sinks. No runtime, analisa dependências e orquestra ordem + paralelismo automaticamente.

---

## 3. Modos de execução

| Aspecto | Triggered | Continuous |
|---|---|---|
| Frequência ideal | 10 min, hora, dia | 10 s a poucos min |
| Processamento | só dados presentes no início | tudo que chega |
| Parada | automática ao concluir | manual |
| Compute | clusters só durante updates → **menor custo** | clusters always-on → **maior custo, menor latência** |

- Materialized views e streaming tables funcionam em ambos os modos
- Em Databricks SQL, refresh de MV/ST é **sempre triggered**
- Tunar continuous: `pipelines.trigger.interval` por tabela ou pipeline-wide (recomendado por tabela — streaming e batch têm defaults diferentes)

**Nosso projeto:** `continuous: true` em `transformations.pipeline.yml` — alinhado com latência de segundos esperada para BI realtime de trades.

---

## 4. Compute

### Serverless (default recomendado)
- Requer **Unity Catalog**
- Requer workspace em região serverless-enabled
- Habilita features CDC e auto-otimização
- **Limitações:** sem R, sem RDD APIs, sem JAR libs, sem DBFS root, sem global temp views

### Classic clusters (só se necessário)
- Use **apenas** se precisar de R, RDD, JAR ou Maven coordinates

**Nosso projeto:** serverless + Photon ativos.

---

## 5. Data Quality — Expectations

Três tipos, por gravidade da violação:

| Tipo | Comportamento | Python | SQL |
|---|---|---|---|
| **Warn** | Registra inválido no target + métrica | `@dp.expect(name, cond)` | `CONSTRAINT n EXPECT (c)` |
| **Drop** | Descarta antes do write | `@dp.expect_or_drop(name, cond)` | `CONSTRAINT n EXPECT (c) ON VIOLATION DROP ROW` |
| **Fail** | Falha o update | `@dp.expect_or_fail(name, cond)` | `CONSTRAINT n EXPECT (c) ON VIOLATION FAIL UPDATE` |

### Regras
- Constraint = SQL Boolean (sem UDF Python, sem chamada externa, sem subquery a outras tabelas)
- Nome único por dataset; reutilizável entre datasets
- Nomes devem comunicar a métrica validada (ex: `valid_price`, não `check_1`)

### Onde ver violações
- UI: **Jobs & Pipelines → Pipeline → Dataset → Data quality tab**
- Event log queries (pra dashboards customizados)

**Nosso projeto (`silver_trades_btc`):**
```python
@dp.expect_or_drop("valid_price", "price > 0")
@dp.expect_or_drop("valid_quantity", "quantity > 0")
@dp.expect_or_drop("valid_side", "taker_side IN ('BUY','SELL')")
@dp.expect_or_drop("not_null_trade_id", "trade_id IS NOT NULL")
@dp.expect("temporal_consistency", "trade_time_ms <= ingested_at_ms")
```

---

## 6. AUTO CDC (SCD Type 1 / Type 2)

Substitui o legado `dlt.apply_changes()`. API:

```python
from pyspark.sql.functions import col
from pyspark import pipelines as dp

dp.create_streaming_table("dim_customers")

dp.create_auto_cdc_flow(
    target="dim_customers",
    source="customers_cdc_clean",
    keys=["customer_id"],
    sequence_by=col("event_timestamp"),     # use col(), não string
    stored_as_scd_type=2,                   # int 2 = Type 2; "1" str = Type 1
    apply_as_deletes=col("op") == "DELETE",
    except_column_list=["op", "_ingested_at"],
    track_history_column_list=["price", "status"],  # SCD2: só essas disparam nova versão
)
```

Lakeflow usa colunas `__START_AT` e `__END_AT` (dois underscores) em SCD2.
Linhas atuais: `WHERE __END_AT IS NULL`.

---

## 7. Channels

- **CURRENT** (default) — versão estável recomendada para produção
- **PREVIEW** — features novas em validação (usar em dev/test)

Configurável no `pipeline.yml` via campo `channel`.

---

## 8. Sintaxe SDP — diferenças vs SQL puro

| Conceito | SDP |
|---|---|
| Criar streaming table SQL | `CREATE OR REFRESH STREAMING TABLE` (não `CREATE OR REPLACE`) |
| Criar MV SQL | `CREATE OR REFRESH MATERIALIZED VIEW` |
| Ler arquivo em streaming | `FROM STREAM read_files(...)` (sem STREAM = batch) |
| Ler tabela em streaming | `FROM stream(my_table)` |
| Particionamento | `CLUSTER BY` (Liquid Clustering) — **não use** `PARTITION BY` + `ZORDER` |
| Metadata de arquivo | `_metadata.file_path` (não `input_file_name()`) |
| Schema target | parâmetro `schema` no pipeline (não `target`) |

---

## 9. Migração da API legada (`dlt` → `pyspark.pipelines`)

| Legado | Moderno |
|---|---|
| `import dlt` | `from pyspark import pipelines as dp` |
| `@dlt.table` | `@dp.table` |
| `dlt.apply_changes()` | `dp.create_auto_cdc_flow()` |
| `dlt.read()` / `dlt.read_stream()` | `spark.read.table(...)` / `spark.readStream.table(...)` |
| `CREATE LIVE TABLE` | `CREATE OR REFRESH STREAMING TABLE` ou `MATERIALIZED VIEW` |

---

## 10. Deploy via Databricks Asset Bundle

Pipeline definido em `resources/*.pipeline.yml`. Estrutura mínima:

```yaml
resources:
  pipelines:
    my_pipeline:
      name: "[${bundle.target}] My Pipeline"
      serverless: true
      continuous: true              # ou false p/ triggered
      catalog: my_catalog
      schema: silver                # schema target default
      photon: true
      channel: CURRENT
      libraries:
        - glob:
            include: ../transformations/**
      configuration:                # parâmetros lidos via spark.conf.get()
        my_param: "value"
      notifications:
        - email_recipients: [me@x.com]
          alerts: [on-update-failure, on-flow-failure]
```

Comandos:
```powershell
databricks bundle validate -t dev --profile dev_deydson
databricks bundle deploy   -t dev --profile dev_deydson
databricks bundle run my_pipeline -t dev --profile dev_deydson --no-wait
```

---

## 11. Aplicação ao projeto analytics-btc

| Camada | Tipo SDP | Arquivo |
|---|---|---|
| Bronze | (não-SDP) tabela alimentada por ZeroBus | `z_bitcoin.bronze.bybit_trades` |
| **Silver** | **Streaming Table** + dedupe c/ watermark + expectations | `transformations/silver_trades_btc.py` ✅ |
| Gold OHLCV 1m | Streaming Table (tumbling window) — a fazer | `transformations/gold_ohlcv_1m.py` |
| Gold Forecast | Materialized View ou Job SQL (`ai_forecast`) — a fazer | `transformations/gold_forecast_btc.py` |

Pipeline único (`transformations.pipeline.yml`) escreve nos schemas `silver` e `gold` via fully-qualified names — preferido sobre múltiplos pipelines.

---

## 12. Referências oficiais

- **Visão geral:** https://docs.databricks.com/aws/en/ldp/
- **Conceitos:** https://docs.databricks.com/aws/en/ldp/concepts
- **SQL reference:** https://docs.databricks.com/aws/en/ldp/developer/sql-dev
- **Python reference:** https://docs.databricks.com/aws/en/ldp/developer/python-ref
- **Loading data:** https://docs.databricks.com/aws/en/ldp/load
- **CDC:** https://docs.databricks.com/aws/en/ldp/cdc
- **Expectations:** https://docs.databricks.com/aws/en/ldp/expectations
- **Triggered vs Continuous:** https://docs.databricks.com/aws/en/ldp/pipeline-mode
