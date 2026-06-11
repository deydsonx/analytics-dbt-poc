# Guia: Criar Dashboard BTC no Databricks

> Passo a passo para configurar o painel de Bitcoin usando Lakeview Dashboards.

## Pré-requisitos

- [ ] Pipeline DLT executado ao menos 1x (dados em `z_bitcoin.gold`)
- [ ] Metric Views criadas (`mtv_btc_intraday`, `mtv_btc_forecast`)
- [ ] Acesso ao workspace Databricks
- [ ] SQL Warehouse disponível (Databricks Runtime 17.2+)

---

## Passo 1: Criar as Metric Views

### 1.1 Executar no SQL Editor

Abra o **SQL Editor** no Databricks e execute cada arquivo:

```bash
# Ordem de execução:
1. src/transformations/03_semantic/mtv_btc_intraday.sql
2. src/transformations/03_semantic/mtv_btc_forecast.sql
```

### 1.2 Validar criação

```sql
-- Verificar se as views existem
SHOW VIEWS IN z_bitcoin.gold LIKE 'mtv_*';

-- Testar query básica
SELECT MEASURE(preco_fechamento) AS ultimo_preco
FROM z_bitcoin.gold.mtv_btc_intraday
WHERE ts_5m >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR;
```

---

## Passo 2: Criar o Dashboard Lakeview

### 2.1 Acessar Lakeview

1. No menu lateral: **Dashboards** > **Create Dashboard**
2. Selecione **Lakeview** (não o legacy)
3. Nome: `PAINEL - Bitcoin (BTC)`

### 2.2 Configurar SQL Warehouse

1. No canto superior direito, clique no seletor de compute
2. Escolha um **SQL Warehouse** com Runtime 17.2+
3. Recomendado: Serverless ou Pro para melhor performance

---

## Passo 3: Criar os Datasets

### Dataset 1: KPIs 24h

**Nome:** `ds_kpis_24h`

```sql
SELECT
    MEASURE(preco_fechamento)        AS ultimo_preco,
    MEASURE(retorno_periodo)         AS variacao_24h_pct,
    MEASURE(volume_usd)              AS volume_24h_usd,
    MEASURE(pressao_compradora_pct)  AS pressao_compra_pct,
    MEASURE(pressao_vendedora_pct)   AS pressao_venda_pct,
    MEASURE(qtd_trades)              AS trades_24h
FROM z_bitcoin.gold.mtv_btc_intraday
WHERE ts_5m >= CURRENT_TIMESTAMP() - INTERVAL 24 HOUR
```

### Dataset 2: Preco Timeline (Historico + Forecast)

**Nome:** `ds_preco_timeline`

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

### Dataset 3: Fluxo Compra/Venda

**Nome:** `ds_fluxo_compra_venda`

```sql
SELECT
    ts_15m,
    MEASURE(volume_compra)  AS volume_compra,
    MEASURE(volume_venda)   AS volume_venda,
    MEASURE(volume_compra) - MEASURE(volume_venda) AS pressao_liquida
FROM z_bitcoin.gold.mtv_btc_intraday
WHERE ts_15m >= CURRENT_TIMESTAMP() - INTERVAL 24 HOUR
GROUP BY ALL
ORDER BY ts_15m
```

### Dataset 4: Tabela Rolling

**Nome:** `ds_rolling_summary`

```sql
WITH janelas AS (
    SELECT '10min' AS janela, 10 AS minutos UNION ALL
    SELECT '20min', 20 UNION ALL
    SELECT '1h', 60 UNION ALL
    SELECT '4h', 240 UNION ALL
    SELECT '1d', 1440
),
dados AS (
    SELECT
        ts_5m,
        MEASURE(preco_abertura)   AS abertura,
        MEASURE(preco_fechamento) AS fechamento,
        MEASURE(volume_btc)       AS volume
    FROM z_bitcoin.gold.mtv_btc_intraday
    WHERE ts_5m >= CURRENT_TIMESTAMP() - INTERVAL 1 DAY
    GROUP BY ALL
)
SELECT
    j.janela,
    MIN(d.abertura) AS abertura_usd,
    MAX(d.fechamento) AS atual_usd,
    (MAX(d.fechamento) - MIN(d.abertura)) / NULLIF(MIN(d.abertura), 0) * 100 AS variacao_pct,
    SUM(d.volume) AS volume_btc,
    CASE
        WHEN MAX(d.fechamento) > MIN(d.abertura) * 1.001 THEN 'Alta'
        WHEN MAX(d.fechamento) < MIN(d.abertura) * 0.999 THEN 'Queda'
        ELSE 'Estavel'
    END AS resultado
FROM janelas j
CROSS JOIN dados d
WHERE d.ts_5m >= CURRENT_TIMESTAMP() - INTERVAL j.minutos MINUTE
GROUP BY j.janela, j.minutos
ORDER BY j.minutos
```

---

## Passo 4: Criar as Visualizações

### 4.1 Cards de KPI (Topo)

| Card | Dataset | Campo | Formato |
|------|---------|-------|---------|
| Ultimo Preco | `ds_kpis_24h` | `ultimo_preco` | Currency USD |
| Variacao 24h | `ds_kpis_24h` | `variacao_24h_pct` | Percentage |
| Volume 24h | `ds_kpis_24h` | `volume_24h_usd` | Currency USD (compact) |
| Pressao Compra | `ds_kpis_24h` | `pressao_compra_pct` | Percentage |

**Configuracao do Card:**
1. Adicione widget **Counter**
2. Selecione o dataset `ds_kpis_24h`
3. Configure o campo de valor
4. Adicione formatacao condicional (verde para alta, vermelho para queda)

### 4.2 Grafico de Preco (Linha)

1. Adicione widget **Line Chart**
2. Dataset: `ds_preco_timeline`
3. Configuracao:
   - X-axis: `ts`
   - Y-axis: `preco`
   - Color: `tipo_dado` (Historico = azul, Previsao = laranja)
4. Para a banda de confianca:
   - Adicione area chart com `limite_inferior` e `limite_superior`
   - Opacidade: 20%
   - Cor: mesma do forecast (laranja)

### 4.3 Grafico Compra vs Venda (Barras Espelhadas)

1. Adicione widget **Bar Chart**
2. Dataset: `ds_fluxo_compra_venda`
3. Configuracao:
   - X-axis: `ts_15m`
   - Y-axis: `volume_compra` (positivo, verde)
   - Y-axis secundario: `-volume_venda` (negativo, vermelho)
   - Linha de pressao liquida sobreposta

### 4.4 Tabela de Distribuicao Rolling

1. Adicione widget **Table**
2. Dataset: `ds_rolling_summary`
3. Colunas:
   | Coluna | Formato | Alinhamento |
   |--------|---------|-------------|
   | janela | Text | Esquerda |
   | abertura_usd | Currency USD | Direita |
   | atual_usd | Currency USD | Direita |
   | variacao_pct | Percentage | Direita |
   | volume_btc | Number (4 decimais) | Direita |
   | resultado | Badge (condicional) | Centro |

4. Formatacao condicional para `resultado`:
   - Alta: Badge verde
   - Queda: Badge vermelho
   - Estavel: Badge cinza

---

## Passo 5: Configurar Layout

### Grid Recomendado

```
+--------------------------------------------------+
|  [Ultimo]  [Var 24h]  [Vol 24h]  [Pressao C/V]   |  <- KPIs
+--------------------------------------------------+
|                                                  |
|          Grafico de Preco + Forecast             |  <- 60% altura
|                                                  |
+--------------------------------------------------+
|  Fluxo Compra/Venda  |  Tabela Rolling           |  <- 40% altura
|        (60%)         |       (40%)               |
+--------------------------------------------------+
```

### Responsividade

1. Clique em **Layout Settings**
2. Configure breakpoints para mobile/tablet
3. Em mobile: empilhar verticalmente

---

## Passo 6: Configurar Filtros

### Filtro Global de Tempo

1. Adicione widget **Filter**
2. Tipo: Date Range
3. Campo: Conecte a todos os datasets que usam filtro temporal
4. Opcoes pre-definidas:
   - Ultimas 24 horas
   - Ultimos 7 dias
   - Ultimo mes

### Excecoes de Filtro

Conforme requisitos, os KPIs e Tabela Rolling sao **estaticos** (sempre 24h). Para isso:

1. Nao conecte o filtro global aos datasets `ds_kpis_24h` e `ds_rolling_summary`
2. Apenas `ds_preco_timeline` e `ds_fluxo_compra_venda` respondem ao filtro

---

## Passo 7: Configurar Refresh

### Auto-Refresh

1. Clique no icone de engrenagem (Settings)
2. **Refresh interval**: 1 minuto
3. Marque **Auto-refresh when viewing**

### Schedule (Opcional)

Para materializar os dados:

1. Acesse **Schedule**
2. Configure refresh a cada 5 minutos
3. Selecione o SQL Warehouse

---

## Passo 8: Publicar e Compartilhar

### Publicar

1. Clique em **Publish**
2. Revise as permissoes
3. Confirme

### Compartilhar

1. Clique em **Share**
2. Adicione usuarios/grupos
3. Defina nivel de acesso:
   - **Viewer**: apenas visualizar
   - **Editor**: editar dashboard
   - **Owner**: gerenciar permissoes

### Embed (Opcional)

1. Clique em **Share** > **Embed**
2. Copie o iframe ou URL publica
3. Configure autenticacao se necessario

---

## Passo 9: Validar

### Checklist de Validacao

- [ ] KPIs mostram dados (nao NULL)
- [ ] Grafico de preco exibe historico + forecast
- [ ] Banda de confianca 95% visivel no forecast
- [ ] Grafico de compra/venda espelhado corretamente
- [ ] Tabela rolling com 5 janelas (10m, 20m, 1h, 4h, 1d)
- [ ] Cores condicionais funcionando (Alta=verde, Queda=vermelho)
- [ ] Auto-refresh funcionando
- [ ] Filtro global afeta apenas graficos (nao KPIs)

### Troubleshooting

| Problema | Solucao |
|----------|---------|
| "No data" | Verificar se pipeline DLT rodou |
| Erro MEASURE() | Verificar Runtime >= 17.2 |
| Dados desatualizados | Verificar job de ingestao |
| Performance lenta | Usar SQL Warehouse Pro/Serverless |

---

## Estrutura Final do Dashboard

```
dashboard/
  dashboard.lvdash.json     <- Exportar apos finalizar
```

Para exportar:
1. Abra o dashboard
2. Clique em **...** > **Export**
3. Salve como `dashboard.lvdash.json`
4. Commit no repositorio

---

## Proximos Passos

1. [ ] Criar alertas de stale data (preco > 5min sem atualizar)
2. [ ] Configurar SQL Alert para lag P95 > 5s
3. [ ] Adicionar metric view para metricas de forecast accuracy
4. [ ] Implementar drill-down por sessao de mercado
