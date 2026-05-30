# ZeroBus SDK - Contexto para Ingestão no Databricks

> Documento de referência para ingestão de dados em tempo real via ZeroBus SDK para Delta Tables.

---

## 1. Visão Geral

**ZeroBus** é um serviço de streaming de alta performance para ingestão direta em Delta Tables no Databricks, otimizado para pipelines real-time e workloads de alto volume.

### 1.1 Características Principais

| Feature | Descrição |
|---------|-----------|
| **Performance** | Backend em Rust com bindings Python (PyO3) |
| **Protocolos** | gRPC, REST API, Arrow Flight, OTLP |
| **Formatos** | JSON, Protocol Buffers, Apache Arrow |
| **APIs** | Sync e Async (asyncio) |
| **Recovery** | Retry automático com backoff exponencial |

### 1.2 SDKs Disponíveis

| Linguagem | Package |
|-----------|---------|
| Python | `databricks-zerobus-ingest-sdk` (PyPI) |
| Rust | `databricks-zerobus-ingest-sdk` (crates.io) |
| Go | `github.com/databricks/zerobus-sdk/go` |
| TypeScript | `@databricks/zerobus-ingest-sdk` (npm) |
| Java | `com.databricks:zerobus-ingest-sdk` (Maven) |

---

## 2. Pré-requisitos

### 2.1 Databricks

1. **Workspace URL**: `https://<instance>.cloud.databricks.com`
2. **Workspace ID**: Número após `/o=` na URL do browser
3. **Unity Catalog** habilitado
4. **Delta Table** criada com schema correspondente

### 2.2 Service Principal

1. Criar em Settings > Identity and Access
2. Gerar Client ID e Client Secret
3. Conceder permissões via SQL:

```sql
-- Permissões necessárias
GRANT USE CATALOG ON CATALOG <catalog> TO `<application-id>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<application-id>`;
GRANT SELECT, MODIFY ON TABLE <catalog>.<schema>.<table> TO `<application-id>`;
```

### 2.3 ZeroBus Endpoint

Formato: `<workspace-id>.zerobus.<region>.cloud.databricks.com`

Exemplo para AWS us-east-1:
```
1234567890123456.zerobus.us-east-1.cloud.databricks.com
```

---

## 3. Instalação Python

```bash
pip install databricks-zerobus-ingest-sdk
```

**Requisitos:**
- Python 3.9+
- `protobuf >= 4.25.0, < 7.0`

**Plataformas suportadas:**
- Linux (x86_64, aarch64)
- macOS (x86_64, arm64)
- Windows (x86_64)

---

## 4. Arquitetura do SDK

### 4.1 Módulos Principais

```python
# API Síncrona
from zerobus.sdk.sync import ZerobusSdk

# API Assíncrona
from zerobus.sdk.aio import ZerobusSdk

# Tipos compartilhados
from zerobus.sdk.shared import (
    RecordType,
    StreamConfigurationOptions,
    TableProperties,
    AckCallback,
    ZerobusException,
    NonRetriableException
)
```

### 4.2 Classes Principais

| Classe | Descrição |
|--------|-----------|
| `ZerobusSdk` | Entry point - cria streams |
| `ZerobusStream` | Gerencia ingestão de records |
| `TableProperties` | Config da tabela destino |
| `StreamConfigurationOptions` | Config do comportamento do stream |
| `AckCallback` | Callback para status de records |

---

## 5. Formatos de Ingestão

### 5.1 JSON (Recomendado para início)

- Schema-free, flexível
- Sem compilação necessária
- Ideal para prototipagem

```python
options = StreamConfigurationOptions(record_type=RecordType.JSON)
record = {"device_name": "sensor-1", "temp": 25}
```

### 5.2 Protocol Buffers (Produção)

- Tipagem forte, validação de schema
- Mais eficiente no wire
- Recomendado para produção

```protobuf
// record.proto
syntax = "proto2";
message Trade {
    optional int64 trade_id = 1;
    optional string symbol = 2;
    optional double price = 3;
}
```

### 5.3 Arrow Flight (Beta)

- Dados colunares/batch
- Melhor para analytics workloads
- API ainda pode mudar

---

## 6. API de Ingestão

### 6.1 Métodos de Ingestão

| Método | Retorno | Uso |
|--------|---------|-----|
| `ingest_record_offset(record)` | `int` | **Recomendado** - retorna offset |
| `ingest_record_nowait(record)` | `None` | Fire-and-forget, máximo throughput |
| `ingest_records_offset(records)` | `int` | Batch com offset |
| `ingest_records_nowait(records)` | `None` | Batch fire-and-forget |

### 6.2 Métodos de Controle

| Método | Descrição |
|--------|-----------|
| `wait_for_offset(offset)` | Bloqueia até record ser durável |
| `flush()` | Aguarda todos records pendentes |
| `close()` | Encerra stream graciosamente |
| `get_unacked_records()` | Retorna records que falharam |
| `get_unacked_batches()` | Retorna batches que falharam |

---

## 7. Configuração do Stream

### 7.1 StreamConfigurationOptions

```python
options = StreamConfigurationOptions(
    record_type=RecordType.JSON,           # JSON ou PROTO
    max_inflight_records=50000,            # Limite de records não confirmados
    recovery=True,                         # Auto-recovery habilitado
    recovery_timeout_ms=15000,             # Timeout de recovery
    recovery_backoff_ms=2000,              # Delay entre retries
    recovery_retries=3,                    # Máximo de retries
    flush_timeout_ms=300000,               # Timeout do flush (5 min)
    server_lack_of_ack_timeout_ms=60000,   # Timeout de ack (1 min)
    ack_callback=MyCallback()              # Callback opcional
)
```

### 7.2 AckCallback (Opcional)

```python
from zerobus.sdk.shared import AckCallback

class MyCallback(AckCallback):
    def on_ack(self, offset: int) -> None:
        print(f"Record {offset} confirmado")
    
    def on_error(self, offset: int, error_message: str) -> None:
        print(f"Record {offset} falhou: {error_message}")
```

---

## 8. Exemplos Completos

### 8.1 JSON Síncrono (Básico)

```python
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties

# Configuração
SERVER_ENDPOINT = "1234567890.zerobus.us-east-1.cloud.databricks.com"
WORKSPACE_URL = "https://my-workspace.cloud.databricks.com"
CLIENT_ID = "your-client-id"
CLIENT_SECRET = "your-client-secret"
TABLE_NAME = "main.bitcoin.btc_trades"

# Criar SDK e stream
sdk = ZerobusSdk(SERVER_ENDPOINT, WORKSPACE_URL)
table_props = TableProperties(TABLE_NAME)
options = StreamConfigurationOptions(record_type=RecordType.JSON)

stream = sdk.create_stream(CLIENT_ID, CLIENT_SECRET, table_props, options)

try:
    # Ingerir records
    for i in range(100):
        record = {
            "trade_id": i,
            "symbol": "BTCUSDT",
            "price": 43250.50 + i,
            "quantity": 0.001
        }
        offset = stream.ingest_record_offset(record)
        print(f"Record {offset} enfileirado")
    
    # Aguardar confirmação
    stream.flush()
    print("Todos records confirmados")

finally:
    stream.close()
```

### 8.2 JSON Assíncrono (Recomendado para WebSocket)

```python
import asyncio
from zerobus.sdk.aio import ZerobusSdk
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties

SERVER_ENDPOINT = "1234567890.zerobus.us-east-1.cloud.databricks.com"
WORKSPACE_URL = "https://my-workspace.cloud.databricks.com"
CLIENT_ID = "your-client-id"
CLIENT_SECRET = "your-client-secret"
TABLE_NAME = "main.bitcoin.btc_trades"

async def main():
    sdk = ZerobusSdk(SERVER_ENDPOINT, WORKSPACE_URL)
    table_props = TableProperties(TABLE_NAME)
    options = StreamConfigurationOptions(record_type=RecordType.JSON)
    
    stream = await sdk.create_stream(CLIENT_ID, CLIENT_SECRET, table_props, options)
    
    try:
        for i in range(100):
            record = {
                "trade_id": i,
                "symbol": "BTCUSDT", 
                "price": 43250.50 + i,
                "quantity": 0.001
            }
            offset = await stream.ingest_record_offset(record)
        
        await stream.flush()
    finally:
        await stream.close()

asyncio.run(main())
```

### 8.3 Com Error Handling e Recovery

```python
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import (
    RecordType, 
    StreamConfigurationOptions, 
    TableProperties,
    ZerobusException,
    NonRetriableException,
    AckCallback
)

class TradeCallback(AckCallback):
    def __init__(self):
        self.acked = 0
        self.failed = 0
    
    def on_ack(self, offset: int) -> None:
        self.acked += 1
    
    def on_error(self, offset: int, error_message: str) -> None:
        self.failed += 1
        print(f"ERRO offset {offset}: {error_message}")

def create_stream(sdk, table_props, options):
    return sdk.create_stream(CLIENT_ID, CLIENT_SECRET, table_props, options)

def ingest_with_recovery(records: list[dict]):
    sdk = ZerobusSdk(SERVER_ENDPOINT, WORKSPACE_URL)
    callback = TradeCallback()
    
    options = StreamConfigurationOptions(
        record_type=RecordType.JSON,
        recovery=True,
        recovery_retries=5,
        ack_callback=callback
    )
    table_props = TableProperties(TABLE_NAME)
    
    stream = create_stream(sdk, table_props, options)
    
    try:
        for record in records:
            try:
                stream.ingest_record_offset(record)
            except NonRetriableException as e:
                # Erro fatal - recriar stream
                print(f"Erro não recuperável: {e}")
                unacked = stream.get_unacked_records()
                stream.close()
                
                # Recriar e reingerir
                stream = create_stream(sdk, table_props, options)
                for rec_bytes in unacked:
                    stream.ingest_record_offset(rec_bytes)
                    
            except ZerobusException as e:
                # Erro recuperável - SDK trata automaticamente
                print(f"Erro recuperável (retry automático): {e}")
        
        stream.flush()
        
    finally:
        stream.close()
        print(f"Acked: {callback.acked}, Failed: {callback.failed}")
```

---

## 9. Integração com WebSocket (Nosso Caso)

### 9.1 Padrão Recomendado: Async WebSocket + Async ZeroBus

```python
import asyncio
import json
import websockets
from zerobus.sdk.aio import ZerobusSdk
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
from datetime import datetime

# Configuração
BINANCE_WS = "wss://stream.binance.com:9443/ws/btcusdt@trade"
SERVER_ENDPOINT = "1234567890.zerobus.us-east-1.cloud.databricks.com"
WORKSPACE_URL = "https://my-workspace.cloud.databricks.com"
CLIENT_ID = "your-client-id"
CLIENT_SECRET = "your-client-secret"
TABLE_NAME = "main.bitcoin.btc_trades"

async def binance_to_databricks():
    # Criar ZeroBus stream
    sdk = ZerobusSdk(SERVER_ENDPOINT, WORKSPACE_URL)
    table_props = TableProperties(TABLE_NAME)
    options = StreamConfigurationOptions(
        record_type=RecordType.JSON,
        max_inflight_records=10000
    )
    
    zerobus_stream = await sdk.create_stream(
        CLIENT_ID, CLIENT_SECRET, table_props, options
    )
    
    try:
        async with websockets.connect(BINANCE_WS) as ws:
            print("Conectado à Binance")
            
            async for message in ws:
                data = json.loads(message)
                
                # Transformar para schema da tabela
                record = {
                    "trade_id": data["t"],
                    "symbol": data["s"],
                    "price": float(data["p"]),
                    "quantity": float(data["q"]),
                    "buyer_order_id": data["b"],
                    "seller_order_id": data["a"],
                    "trade_time": data["T"],
                    "is_buyer_maker": data["m"],
                    "ingested_at": int(datetime.now().timestamp() * 1000)
                }
                
                # Ingerir no Databricks
                await zerobus_stream.ingest_record_nowait(record)
                
    except Exception as e:
        print(f"Erro: {e}")
        await zerobus_stream.flush()
    finally:
        await zerobus_stream.close()

if __name__ == "__main__":
    asyncio.run(binance_to_databricks())
```

---

## 10. Variáveis de Ambiente

```bash
# Credenciais
export DATABRICKS_CLIENT_ID="your-client-id"
export DATABRICKS_CLIENT_SECRET="your-client-secret"

# Endpoints
export ZEROBUS_SERVER_ENDPOINT="1234567890.zerobus.us-east-1.cloud.databricks.com"
export DATABRICKS_WORKSPACE_URL="https://my-workspace.cloud.databricks.com"

# Tabela
export ZEROBUS_TABLE_NAME="main.bitcoin.btc_trades"

# Proxy (opcional)
export https_proxy="http://my-proxy:8080"
export no_proxy="localhost,127.0.0.1"
```

---

## 11. Criação da Tabela Delta

### 11.1 Schema para Trades BTC

```sql
CREATE TABLE main.bitcoin.btc_trades (
    trade_id        BIGINT          COMMENT 'Binance trade ID',
    symbol          STRING          COMMENT 'Trading pair (BTCUSDT)',
    price           DECIMAL(18,8)   COMMENT 'Trade price',
    quantity        DECIMAL(18,8)   COMMENT 'Trade quantity',
    buyer_order_id  BIGINT          COMMENT 'Buyer order ID',
    seller_order_id BIGINT          COMMENT 'Seller order ID',
    trade_time      BIGINT          COMMENT 'Trade timestamp (ms)',
    is_buyer_maker  BOOLEAN         COMMENT 'Was buyer the maker?',
    ingested_at     BIGINT          COMMENT 'Ingestion timestamp (ms)'
)
USING DELTA
PARTITIONED BY (date(from_unixtime(trade_time/1000)))
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
```

### 11.2 Permissões do Service Principal

```sql
-- Substituir <app-id> pelo UUID do Service Principal
GRANT USE CATALOG ON CATALOG main TO `<app-id>`;
GRANT USE SCHEMA ON SCHEMA main.bitcoin TO `<app-id>`;
GRANT SELECT, MODIFY ON TABLE main.bitcoin.btc_trades TO `<app-id>`;
```

---

## 12. Troubleshooting

### 12.1 Erros Comuns

| Erro | Causa | Solução |
|------|-------|---------|
| Permission denied | SP sem permissões | Verificar GRANT SELECT, MODIFY |
| Token expired | OAuth expirou | SDK renova automaticamente |
| Connection refused | Endpoint errado | Verificar formato do endpoint |
| Schema mismatch | Campos não batem | Ajustar record ou tabela |

### 12.2 Tipos de Exceção

```python
# Erro recuperável - SDK trata com retry
ZerobusException

# Erro fatal - precisa recriar stream
NonRetriableException
```

---

## 13. Performance Tuning

### 13.1 Recomendações

| Parâmetro | Valor Recomendado | Quando Ajustar |
|-----------|-------------------|----------------|
| `max_inflight_records` | 50000 (default) | Aumentar para alto throughput |
| `recovery_retries` | 3-5 | Aumentar em redes instáveis |
| `flush_timeout_ms` | 300000 | Ajustar por batch size |

### 13.2 Throughput por Protocolo

| Protocolo | Throughput | Uso |
|-----------|------------|-----|
| gRPC | **Máximo** (40x Python puro) | Streaming contínuo |
| REST | Menor | Dispositivos edge |
| Arrow | Alto (batch) | Dados colunares |

---

## 14. Monitoramento

### 14.1 Métricas via Callback

```python
class MetricsCallback(AckCallback):
    def __init__(self):
        self.total_acked = 0
        self.total_failed = 0
        self.last_offset = 0
    
    def on_ack(self, offset: int) -> None:
        self.total_acked += 1
        self.last_offset = offset
    
    def on_error(self, offset: int, error_message: str) -> None:
        self.total_failed += 1
        # Enviar para sistema de alertas
```

### 14.2 System Tables (Databricks)

O ZeroBus popula system tables com métricas de:
- Stream health
- Ingest throughput
- Error rates
- Protocol distribution

---

## Referências

- [ZeroBus SDK GitHub](https://github.com/databricks/zerobus-sdk)
- [ZeroBus Python SDK](https://github.com/databricks/zerobus-sdk-py)
- [Documentação Oficial Databricks](https://docs.databricks.com/aws/en/ingestion/zerobus-ingest)
- [Exemplos Oficiais](https://github.com/databricks-solutions/zerobus-ingest-examples)
- [ZeroBus Overview](https://docs.databricks.com/aws/en/ingestion/zerobus-overview)
