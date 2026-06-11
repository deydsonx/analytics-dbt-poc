# Guia de Metric Views - Databricks

> Referência completa para criar e consumir Metric Views no projeto analytics-btc.

## Sumário

1. [O que são Metric Views](#o-que-são-metric-views)
2. [Sintaxe YAML (Criar)](#sintaxe-yaml-criar)
3. [Sintaxe SQL (Consumir)](#sintaxe-sql-consumir)
4. [Metric Views do Projeto](#metric-views-do-projeto)
5. [Queries para o Dashboard](#queries-para-o-dashboard)

---

## O que são Metric Views

Metric Views são uma camada semântica do Unity Catalog que separa a **definição de métricas** dos **agrupamentos e filtros**. Você define a métrica uma vez (ex: `SUM(volume) / COUNT(*)`) e os usuários podem agrupar por qualquer dimensão disponível em runtime.

**Benefícios:**
- Definição única de métricas (Single Source of Truth)
- Agrupamento flexível em query time
- Integração com AI/BI, Genie, dashboards, alerts
- Synonyms para descoberta via linguagem natural

---

## Sintaxe YAML (Criar)

### Estrutura Básica

```sql
CREATE OR REPLACE VIEW catalog.schema.nome_view
WITH METRICS LANGUAGE YAML
AS $$
version: 1.1
comment: "Descrição da metric view"

source: catalog.schema.tabela_origem

filter: coluna > 0  -- opcional, aplica em todas as queries

fields:  -- ou "dimensions" (backward compat)
  - name: nome_campo
    expr: expressao_sql
    display_name: "Nome Amigável"
    comment: "Descrição do campo"
    synonyms:
      - "sinonimo1"
      - "sinonimo2"

measures:
  - name: nome_metrica
    expr: SUM(coluna)  -- DEVE usar função agregada
    display_name: "Nome Amigável"
    comment: "Descrição da métrica"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "sinonimo"
$$;
```

### Propriedades de Fields (Dimensões)

| Propriedade | Tipo | Obrigatório | Descrição |
|-------------|------|-------------|-----------|
| `name` | String | Sim | Alias do campo |
| `expr` | String | Sim | Expressão SQL |
| `display_name` | String | Não | Nome amigável (max 255 chars) |
| `comment` | String | Não | Descrição no Unity Catalog |
| `synonyms` | Array | Não | Até 10 sinônimos (255 chars cada) |
| `format` | Map | Não | Formatação de exibição |

### Propriedades de Measures (Métricas)

| Propriedade | Tipo | Obrigatório | Descrição |
|-------------|------|-------------|-----------|
| `name` | String | Sim | Alias da métrica |
| `expr` | String | Sim | Expressão com função agregada (SUM, COUNT, AVG, MIN, MAX, MEDIAN, PERCENTILE) |
| `display_name` | String | Não | Nome amigável |
| `comment` | String | Não | Descrição |
| `format` | Map | Não | Formatação |
| `synonyms` | Array | Não | Sinônimos |
| `window` | Array | Não | Para window measures |

### Formato de Métricas

```yaml
format:
  type: currency  # currency | percentage | number
  currency_code: USD  # apenas para currency
  decimal_places:
    type: exact  # exact | auto
    places: 2
```

### Filtros Condicionais em Métricas

```yaml
measures:
  - name: receita_urgente
    expr: SUM(valor) FILTER (WHERE prioridade = 'URGENTE')
```

---

## Sintaxe SQL (Consumir)

### Regra Crítica: MEASURE()

**Toda métrica deve ser envolvida pela função `MEASURE()` ao consultar!**

```sql
-- ERRADO (vai falhar)
SELECT ts_15m, volume_usd 
FROM mtv_btc_intraday 
GROUP BY ts_15m;

-- CORRETO
SELECT ts_15m, MEASURE(volume_usd) 
FROM mtv_btc_intraday 
GROUP BY ALL;
```

### Padrões de Query

```sql
-- Dimensões + Métricas com GROUP BY ALL
SELECT 
    dimensao1,
    dimensao2,
    MEASURE(metrica1),
    MEASURE(metrica2)
FROM metric_view
WHERE dimensao1 > '2024-01-01'
GROUP BY ALL
ORDER BY dimensao1;

-- Apenas métricas (sem GROUP BY)
SELECT 
    MEASURE(metrica1),
    MEASURE(metrica2)
FROM metric_view
WHERE dimensao1 > '2024-01-01';

-- Operações entre métricas
SELECT 
    ts_15m,
    MEASURE(volume_compra) - MEASURE(volume_venda) AS pressao_liquida
FROM metric_view
GROUP BY ALL;
```

### Limitações

1. **Não use `SELECT *`** - liste campos explicitamente
2. **Metric views não podem ser JOINadas diretamente** - use CTE primeiro:
   ```sql
   WITH mv AS (
       SELECT ts_15m, MEASURE(volume_usd) AS vol
       FROM metric_view
       GROUP BY ALL
   )
   SELECT mv.*, t.outra_coluna
   FROM mv JOIN outra_tabela t ON mv.ts_15m = t.ts;
   ```

---

## Metric Views do Projeto

### mtv_btc_intraday

**Fonte:** `z_bitcoin.gold.f_trade_analytics` (1 minuto)

**Dimensões:**
| Nome | Descrição |
|------|-----------|
| `symbol` | Ativo (BTCUSDT) |
| `ts_5m` | Horário truncado 5min |
| `ts_10m` | Horário truncado 10min |
| `ts_15m` | Horário truncado 15min |
| `ts_20m` | Horário truncado 20min |
| `ts_30m` | Horário truncado 30min |
| `ts_1h` | Horário truncado 1h |
| `ts_4h` | Horário truncado 4h |
| `data` | Data (sem hora) |
| `sessao_mercado` | Asia / Europa / Americas |

**Métricas Principais:**
| Nome | Descrição |
|------|-----------|
| `preco_abertura` | Preço de abertura (OHLC) |
| `preco_fechamento` | Preço de fechamento |
| `preco_maximo` | Preço máximo |
| `preco_minimo` | Preço mínimo |
| `vwap` | Preço médio ponderado por volume |
| `volume_btc` | Volume em BTC |
| `volume_usd` | Volume em USD |
| `volume_compra` | Volume de compras |
| `volume_venda` | Volume de vendas |
| `desequilibrio_taker` | Pressão compradora vs vendedora |
| `pressao_compradora_pct` | % do volume de compra |
| `pressao_vendedora_pct` | % do volume de venda |
| `retorno_periodo` | Variação % do período |
| `tipo_candle` | Bullish / Bearish / Doji |

### mtv_btc_forecast

**Fonte:** `z_bitcoin.gold.f_forecast` (AI_FORECAST, 24h)

**Dimensões:**
| Nome | Descrição |
|------|-----------|
| `symbol` | Ativo |
| `forecast_ts_15m` | Horário da previsão 15min |
| `forecast_ts_30m` | Horário da previsão 30min |
| `forecast_ts_1h` | Horário da previsão 1h |
| `forecast_ts_4h` | Horário da previsão 4h |
| `horizonte_bucket` | Faixa de horizonte (0-1h, 1-4h, etc.) |
| `data_previsao` | Data da previsão |

**Métricas Principais:**
| Nome | Descrição |
|------|-----------|
| `preco_previsto` | Previsão pontual (USD) |
| `limite_inferior_95` | Limite inferior IC 95% |
| `limite_superior_95` | Limite superior IC 95% |
| `largura_intervalo_pct` | Incerteza relativa |
| `variacao_prevista_pct` | Variação esperada |
| `sinal_tendencia` | Alta / Baixa / Neutro |

---

## Queries para o Dashboard

### 1. KPIs (Cards de Topo)

```sql
-- Último Preço, Variação 24h, Volume 24h, Pressão Compra/Venda
SELECT
    MEASURE(preco_fechamento)        AS ultimo_preco,
    MEASURE(retorno_periodo)         AS variacao_24h_pct,
    MEASURE(volume_usd)              AS volume_24h_usd,
    MEASURE(pressao_compradora_pct)  AS pressao_compra_pct,
    MEASURE(pressao_vendedora_pct)   AS pressao_venda_pct
FROM z_bitcoin.gold.mtv_btc_intraday
WHERE ts_5m >= CURRENT_TIMESTAMP() - INTERVAL 24 HOUR
```

### 2. Gráfico Preço Timeline (Histórico + Forecast)

```sql
WITH historico AS (
    SELECT
        ts_15m AS ts,
        'Historico' AS tipo_dado,
        MEASURE(preco_fechamento) AS preco,
        CAST(NULL AS DOUBLE) AS limite_inferior,
        CAST(NULL AS DOUBLE) AS limite_superior
    FROM z_bitcoin.gold.mtv_btc_intraday
    WHERE ts_15m >= CURRENT_TIMESTAMP() - INTERVAL 7 DAY
    GROUP BY ALL
),
forecast AS (
    SELECT
        forecast_ts_15m AS ts,
        'Previsao' AS tipo_dado,
        MEASURE(preco_previsto) AS preco,
        MEASURE(limite_inferior_95) AS limite_inferior,
        MEASURE(limite_superior_95) AS limite_superior
    FROM z_bitcoin.gold.mtv_btc_forecast
    GROUP BY ALL
)
SELECT * FROM historico
UNION ALL
SELECT * FROM forecast
ORDER BY ts
```

### 3. Gráfico Fluxo Compra vs Venda

```sql
SELECT
    ts_15m,
    MEASURE(volume_compra)                              AS volume_compra,
    -MEASURE(volume_venda)                              AS volume_venda_neg,
    MEASURE(volume_compra) - MEASURE(volume_venda)      AS pressao_liquida
FROM z_bitcoin.gold.mtv_btc_intraday
WHERE ts_15m >= CURRENT_TIMESTAMP() - INTERVAL 24 HOUR
GROUP BY ALL
ORDER BY ts_15m
```

### 4. Tabela Distribuição Rolling

```sql
-- Exemplo para janela de 1 hora
SELECT
    '1h' AS janela,
    MEASURE(preco_abertura)    AS abertura_usd,
    MEASURE(preco_fechamento)  AS atual_usd,
    MEASURE(retorno_periodo)   AS variacao_pct,
    MEASURE(volume_btc)        AS volume_btc,
    MEASURE(tipo_candle)       AS resultado
FROM z_bitcoin.gold.mtv_btc_intraday
WHERE ts_1h >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
```

---

## Referências

- [Query metric views](https://docs.databricks.com/aws/en/business-semantics/metric-views/query)
- [Metric view YAML syntax reference](https://docs.databricks.com/aws/en/business-semantics/metric-views/yaml-reference)
- [Use SQL to create metric views](https://docs.databricks.com/aws/en/metric-views/create/sql)
- [Model metric views](https://docs.databricks.com/aws/en/business-semantics/metric-views/basic-modeling)
