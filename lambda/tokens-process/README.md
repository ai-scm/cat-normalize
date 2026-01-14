# 🪙 Tokens Processing - Dual Lambda Architecture

Sistema de análisis de tokens de Amazon Bedrock con arquitectura dual para procesamiento histórico y consolidado en múltiples ambientes.

## 📋 Índice

- [Descripción General](#-descripción-general)
- [Arquitectura](#-arquitectura)
- [Lambdas](#-lambdas)
- [Configuración](#-configuración)
- [Flujo de Datos](#-flujo-de-datos)
- [Deployment](#-deployment)
- [Uso](#-uso)
- [Troubleshooting](#-troubleshooting)

## 🎯 Descripción General

Este módulo procesa datos de tokens de conversaciones de Amazon Bedrock Claude en dos fases:

1. **Fase Histórica (One-time)**: Procesa datos antiguos de una tabla DynamoDB legacy
2. **Fase Consolidada (Daily)**: Procesa datos nuevos y los consolida con los históricos

### Características Principales

- ✅ **Dual Lambda**: Dos funciones independientes para históricos y actuales
- ✅ **Multi-Ambiente**: Configuración TypeScript para test/prod
- ✅ **Consolidación**: Une datos de ambas fuentes en un solo CSV
- ✅ **Athena Integration**: Actualiza vistas automáticamente
- ✅ **Precisión**: Usa DECIMAL(10,6) para costos exactos
- ✅ **Soporte v2**: Maneja nuevos tipos `toolUse` y `toolResult`

## 🏗️ Arquitectura

```
┌─────────────────────────────────────────────────────────────────┐
│                    PROCESAMIENTO HISTÓRICO                       │
│                        (ONE-TIME)                                │
└─────────────────────────────────────────────────────────────────┘

┌──────────────────┐         ┌─────────────────────────────┐
│  DynamoDB        │         │  Lambda Archival            │
│  Tabla Antigua   │────────▶│  (One-time Execution)       │
│                  │         │                             │
│  Format: Legacy  │         │  Handler:                   │
│  - Simple arrays │         │  lambda-tokens-archival-    │
│  - No toolUse    │         │  processing.py              │
│                  │         │                             │
│  Date Range:     │         │  Logic: Original v1         │
│  2024-01-01 to   │         │  - Extract from content[]   │
│  2025-11-30      │         │  - Calculate tokens/4       │
└──────────────────┘         │  - Simple role checks       │
                             └─────────────────────────────┘
                                        │
                                        ▼
                             ┌─────────────────────────────┐
                             │  S3: Historical Data        │
                             │  tokens_analysis_old_table  │
                             │  .csv                       │
                             │                             │
                             │  Columns:                   │
                             │  - create_date              │
                             │  - input_token              │
                             │  - output_token             │
                             │  - total_price              │
                             │  - source: 'old_table'      │
                             └─────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                  PROCESAMIENTO CONSOLIDADO                       │
│                        (DAILY)                                   │
└─────────────────────────────────────────────────────────────────┘

┌──────────────────┐         ┌─────────────────────────────┐
│  DynamoDB        │         │  Lambda Consolidated        │
│  Tabla Nueva     │────────▶│  (Daily Execution)          │
│                  │         │                             │
│  Format: New v2  │         │  Handler:                   │
│  - Nested msgs   │         │  tokens_lambda.py           │
│  - toolUse       │         │                             │
│  - toolResult    │         │  Logic: Enhanced v2         │
│                  │         │  - Handle toolUse/Result    │
│  Date Range:     │         │  - Recursive processing     │
│  2025-12-01+     │         │  - Type-specific handlers   │
└──────────────────┘         └─────────────────────────────┘
                                        │
                                        ├──────────────┐
                                        ▼              ▼
                             ┌──────────────┐  ┌──────────────┐
                             │ Read         │  │ Process New  │
                             │ Historical   │  │ Data         │
                             │ CSV from S3  │  │ from DynamoDB│
                             └──────────────┘  └──────────────┘
                                        │              │
                                        └──────┬───────┘
                                               ▼
                             ┌─────────────────────────────┐
                             │  CONSOLIDATION              │
                             │  - Merge both sources       │
                             │  - Add 'source' column      │
                             │  - Remove duplicates        │
                             │  - Sort by date DESC        │
                             └─────────────────────────────┘
                                        │
                                        ▼
                             ┌─────────────────────────────┐
                             │  S3: Consolidated Data      │
                             │  tokens_analysis_           │
                             │  consolidated.csv           │
                             │                             │
                             │  Columns: Same + source     │
                             │  - source: 'old_table'      │
                             │  - source: 'new_table'      │
                             └─────────────────────────────┘
                                        │
                                        ▼
                             ┌─────────────────────────────┐
                             │  Athena View Auto-Update    │
                             │  cat_{env}_token_usage_     │
                             │  analysis                   │
                             └─────────────────────────────┘
```

## 🐍 Lambdas

### Lambda 1: Archival Processing (One-time)

**Archivo**: `lambda-tokens-archival-processing.py`

**Propósito**: Procesa la tabla antigua (histórica) una sola vez.

**Características**:
- ✅ Usa lógica **original v1** (compatible con formato legacy)
- ✅ Procesa arrays simples de contenido
- ✅ No maneja tipos nuevos (toolUse/toolResult)
- ✅ Extrae texto directamente de `content[].body`
- ✅ Filtra por rango de fechas configurable
- ✅ Genera CSV estático en S3

**Configuración**:
```python
# Environment Variables
OLD_DYNAMODB_TABLE_NAME = 'BedrockChatStack-Old-Table'
S3_BUCKET_NAME = 'cat-{env}-normalize-reports'
S3_OLD_DATA_PREFIX = 'archives/tokens-analysis/'
FILTER_DATE_START = '2025-08-04'
FILTER_DATE_END = '2025-12-31'
```

**Lógica de Tokens v1**:
```python
def extract_tokens_from_messagemap_OLD(message_map):
    """
    Algoritmo original para formato legacy
    """
    input_tokens = 0
    output_tokens = 0
    
    for key, value in message_map.items():
        role = value.get('role', '').lower()
        content = value.get('content', [])
        
        # Procesar content como lista
        if isinstance(content, list):
            for item in content:
                if isinstance(item, dict) and 'body' in item:
                    body = item.get('body', '')
                    tokens = len(body) // 4
                    
                    if role in ['user', 'system']:
                        input_tokens += tokens
                    elif role in ['assistant', 'bot']:
                        output_tokens += tokens
    
    return input_tokens, output_tokens
```

**Ejecución**:
- **Frecuencia**: Una sola vez por ambiente
- **Trigger**: Manual (AWS Console o CLI)
- **Timeout**: 900 segundos (15 minutos)
- **Memory**: 1024 MB (test), 2048 MB (prod)

**Output**:
```
s3://{bucket}/archives/tokens-analysis/tokens_analysis_old_table.csv
```

### Lambda 2: Consolidated Processing (Daily)

**Archivo**: `tokens_lambda.py`

**Propósito**: Procesa la tabla nueva diariamente y consolida con históricos.

**Características**:
- ✅ Usa lógica **mejorada v2** (soporte formato nuevo)
- ✅ Maneja estructuras anidadas de mensajes
- ✅ Procesa tipos nuevos: `toolUse`, `toolResult`
- ✅ Procesamiento recursivo de contenido
- ✅ Consolida con datos históricos
- ✅ Actualiza vista Athena automáticamente

**Configuración**:
```python
# Environment Variables
DYNAMODB_TABLE_NAME = 'BedrockChatStack-New-Table'
S3_BUCKET_NAME = 'cat-{env}-normalize-reports'
S3_OUTPUT_PREFIX = 'tokens-analysis/'
S3_OLD_DATA_PREFIX = 'archives/tokens-analysis/'
ATHENA_DATABASE = 'cat_{env}_analytics_db'
ATHENA_WORKGROUP = 'wg-cat-{env}-analytics'
FILTER_DATE_START = '2025-12-01'  # Fecha de migración
```

**Lógica de Tokens v2**:
```python
def extract_tokens_from_messagemap_v2(message_map):
    """
    Algoritmo mejorado para formato nuevo
    Soporta: toolUse, toolResult, nested messages
    """
    input_tokens = 0
    output_tokens = 0
    
    for node_id, node_data in message_map.items():
        role = node_data.get('role')
        content = node_data.get('content', [])
        
        # Procesar contenido recursivamente
        node_input, node_output = process_node_content(content, role)
        input_tokens += node_input
        output_tokens += node_output
    
    return input_tokens, output_tokens

def process_content_item(item, parent_role):
    """
    Procesa items por tipo
    """
    content_type = item.get('content_type', 'text')
    
    if content_type == 'text':
        # Texto normal
        tokens = calculate_tokens(item.get('body', ''))
        
    elif content_type == 'toolUse':
        # Tool invocation (OUTPUT)
        tool_input = item.get('body', {}).get('input', {})
        tokens = calculate_tokens(json.dumps(tool_input))
        return 0, tokens  # OUTPUT tokens
        
    elif content_type == 'toolResult':
        # Tool response (INPUT)
        result_content = item.get('body', {}).get('content', [])
        tokens = sum([calculate_tokens(str(r)) for r in result_content])
        return tokens, 0  # INPUT tokens
```

**Proceso de Consolidación**:
```python
def consolidar_datos(datos_nuevos, df_historico):
    """
    1. Convierte datos nuevos a DataFrame
    2. Lee históricos desde S3
    3. Concatena ambos
    4. Elimina duplicados (por pk, sk)
    5. Ordena por fecha DESC
    6. Retorna DataFrame consolidado
    """
    df_nuevos = pd.DataFrame(datos_nuevos)
    df_consolidado = pd.concat([df_historico, df_nuevos])
    df_consolidado = df_consolidado.drop_duplicates(subset=['pk', 'sk'])
    return df_consolidado.sort_values('create_date', ascending=False)
```

**Ejecución**:
- **Frecuencia**: Diaria
- **Trigger**: EventBridge Schedule `cron(30 4 * * ? *)`
- **Timeout**: 300 segundos (5 minutos)
- **Memory**: 512 MB

**Output**:
```
s3://cat-{env}-normalize-reports/tokens-analysis/tokens_analysis_consolidated.csv
```

## ⚙️ Configuración

### Configuración por Ambiente (TypeScript)

**Interface**: `lib/configs/tokens-config.interface.ts`

```typescript
export interface TokensConfig {
  environment: 'test' | 'prod';
  
  // DynamoDB Tables
  oldDynamoTableName: string;
  newDynamoTableName: string;
  
  // S3 Configuration
  outputBucket: string;
  outputPrefix: string;
  historicalPrefix: string;
  
  // Lambda Configuration
  archivalLambda: {
    name: string;
    timeout: number;  // seconds
    memorySize: number;  // MB
  };
  
  consolidatedLambda: {
    name: string;
    timeout: number;
    memorySize: number;
  };
  
  // Date Filters
  dateFilter: {
    archivalStart: string;    // YYYY-MM-DD
    archivalEnd: string;      // YYYY-MM-DD
    consolidatedStart: string; // YYYY-MM-DD
  };
  
  // Schedule
  schedule: {
    enabled: boolean;
    cronExpression: string;
  };
}
```

## 🔄 Flujo de Datos

### Paso 1: Procesamiento Histórico (One-time)

```
1. Lambda Archival se ejecuta MANUALMENTE
   ├─ Lee DynamoDB tabla antigua
   ├─ Filtra por FILTER_DATE_START/END
   ├─ Aplica lógica v1 (original)
   ├─ Calcula tokens: LENGTH(text) / 4
   ├─ Genera estadísticas
   └─ Sube CSV a S3 historical/

Output: s3:/cat-{env}-normalize-reportsarchives/tokens-analysis/tokens_analysis_old_table.csv

Columns:
- create_date: '2025-01-08 14:30:45'
- input_token: 1234
- output_token: 5678
- precio_token_input: 0.003702
- precio_token_output: 0.085170
- total_price: 0.088872
- pk: 'PK#123'
- sk: 'SK#456'
- source: 'old_table'
```

### Paso 2: Procesamiento Diario (Daily)

```
1. Lambda Consolidated se ejecuta DIARIAMENTE (solo prod)
   ├─ Lee DynamoDB tabla nueva
   ├─ Filtra desde FILTER_DATE_START (fecha migración)
   ├─ Aplica lógica v2 (mejorada)
   │  ├─ Detecta content_type
   │  ├─ Procesa toolUse → OUTPUT tokens
   │  ├─ Procesa toolResult → INPUT tokens
   │  └─ Maneja nested messages recursivamente
   │
   ├─ Lee CSV histórico desde S3
   │
   ├─ CONSOLIDA ambas fuentes
   │  ├─ pd.concat([históricos, nuevos])
   │  ├─ drop_duplicates(pk, sk)
   │  └─ sort_values('create_date', DESC)
   │
   ├─ Genera CSV consolidado
   │
   └─ Actualiza vista Athena

Output: s3:/cat-{env}-normalize-reportstokens-analysis/tokens_analysis_consolidated.csv

Contiene AMBAS fuentes:
- Registros de old_table (source='old_table')
- Registros de new_table (source='new_table')
```

### Paso 3: Athena View Auto-Update

```sql
CREATE OR REPLACE VIEW cat_{env}_token_usage_analysis AS
SELECT
    create_date,
    input_token AS "token pregunta",
    output_token AS "token respuesta",
    input_token + output_token AS "total tokens",
    precio_token_input AS "precio total pregunta",
    precio_token_output AS "precio total respuesta",
    total_price AS "precio total",
    source AS "origen datos"
FROM tokens_table_consolidated
WHERE input_token > 0 OR output_token > 0
ORDER BY create_date DESC;
```

## 🚀 Deployment

### Deploy a Test

```bash
# Synthesize
cdk synth CatTokensStack-test -c environment=test

# Diff
cdk diff CatTokensStack-test -c environment=test

# Deploy
cdk deploy CatTokensStack-test -c environment=test
```

### Deploy a Prod

```bash
cdk deploy CatTokensStack-prod -c environment=prod
```

### Post-Deployment: Ejecutar Archival Lambda (One-time)

```bash
# Test
aws lambda invoke \
  --function-name cat-test-lambda-tokens-archival \
  --payload '{}' \
  response.json

# Prod
aws lambda invoke \
  --function-name cat-prod-lambda-tokens-archival \
  --payload '{}' \
  response.json

# Verificar output
cat response.json | jq '.body | fromjson'
```

**Output esperado**:
```json
{
  "statusCode": 200,
  "body": {
    "message": "Procesamiento de tabla antigua completado exitosamente",
    "statistics": {
      "total_records": 12543,
      "total_input_tokens": 23456789,
      "total_output_tokens": 34567890,
      "total_cost": 152.34
    },
    "s3_file": "s3:/cat-{env}-normalize-reportsarchives/tokens-analysis/tokens_analysis_old_table.csv"
  }
}
```

## 📊 Uso

### Queries Athena Útiles

```sql
-- 1. Conteo por origen
SELECT 
  source,
  COUNT(*) as records,
  SUM(input_token) as input_tokens,
  SUM(output_token) as output_tokens,
  SUM(total_price) as total_cost
FROM tokens_table
GROUP BY source;

-- 2. Análisis diario consolidado
SELECT 
  DATE(create_date) as date,
  source,
  COUNT(*) as conversations,
  SUM(total_price) as daily_cost
FROM tokens_table_consolidated
GROUP BY DATE(create_date), source
ORDER BY date DESC, source;

-- 3. Estadísticas por hora
SELECT 
  HOUR(create_date) as hour,
  AVG(input_token) as avg_input,
  AVG(output_token) as avg_output,
  AVG(total_price) as avg_cost
FROM tokens_table
GROUP BY HOUR(create_date)
ORDER BY hour;

-- 4. Top 10 conversaciones más costosas
SELECT 
  create_date,
  input_token,
  output_token,
  total_price,
  source
FROM tokens_table
ORDER BY total_price DESC
LIMIT 10;

-- 5. Verificar consolidación correcta
SELECT 
  source,
  MIN(create_date) as earliest,
  MAX(create_date) as latest,
  COUNT(*) as count
FROM tokens_table
GROUP BY source;
```

### Verificar Archivos S3

```bash
# Listar todos los archivos
aws s3 ls s3://cat-prod-normalize-reports/tokens-analysis/ --recursive

# Descargar histórico
aws s3 cp s3://cat-prod-normalize-reports/archival/tokens-analysis/tokens_analysis_old_table.csv .

# Descargar consolidado
aws s3 cp s3://cat-prod-normalize-reports/tokens-analysis/tokens_analysis_consolidated.csv .

# Verificar formato
head -5 tokens_analysis_consolidated.csv
grep -c "old_table" tokens_analysis_consolidated.csv
grep -c "new_table" tokens_analysis_consolidated.csv
```

### Monitoreo CloudWatch

```bash
# Lambda Archival
aws logs tail /aws/lambda/cat-prod-lambda-tokens-archival --follow

# Lambda Consolidated
aws logs tail /aws/lambda/cat-prod-lambda-tokens-consolidated --follow

# Filtrar errores
aws logs filter-log-events \
  --log-group-name /aws/lambda/cat-prod-lambda-tokens-consolidated \
  --filter-pattern "ERROR"
```

## 🐛 Troubleshooting

### Problema: Archival Lambda timeout

**Síntoma**: Lambda excede 15 minutos

**Solución**:
```bash
# Aumentar memory (más CPU)
# Editar lib/configs/prod-tokens.config.ts
archivalLambda: {
  memorySize: 3008  // Aumentar a 3GB
}

# Redeploy
cdk deploy CatTokensStack-prod -c environment=prod
```

### Problema: Athena error "ResultConfiguration cannot be set"

**Causa**: WorkGroup tiene resultados managed

**Solución**: Ya corregido en `tokens_lambda.py`:
```python
# ❌ ANTES (causaba error)
response = athena_client.start_query_execution(
    QueryString=query,
    QueryExecutionContext={'Database': ATHENA_DATABASE},
    ResultConfiguration={'OutputLocation': ATHENA_OUTPUT_LOCATION},  # ❌
    WorkGroup=ATHENA_WORKGROUP
)

# ✅ AHORA (correcto)
response = athena_client.start_query_execution(
    QueryString=query,
    QueryExecutionContext={'Database': ATHENA_DATABASE},
    WorkGroup=ATHENA_WORKGROUP  # Solo WorkGroup
)
```

### Problema: CSV header parsed as data

**Causa**: Athena table sin `skip.header.line.count`

**Solución**:
```sql
ALTER TABLE tokens_table_consolidated 
SET TBLPROPERTIES ('skip.header.line.count'='1');
```

### Problema: Historical file not found

**Causa**: Lambda Archival no ejecutado

**Solución**:
```bash
# Ejecutar Lambda Archival primero
aws lambda invoke \
  --function-name cat-prod-lambda-tokens-archival \
  --payload '{}' \
  response.json

# Verificar archivo
aws s3 ls s3://cat-prod-normalize-reports/archives/tokens-analysis/
```

### Problema: Duplicados en consolidated

**Causa**: Drop duplicates no funcionó

**Solución**: Revisar logs para verificar:
```python
# En tokens_lambda.py se hace:
df_consolidado = df_consolidado.drop_duplicates(subset=['pk', 'sk'], keep='first')
```

## 📈 Métricas y Performance

| Métrica | Lambda Archival | Lambda Consolidated |
|---------|-----------------|---------------------|
| **Execution Time** | 5-15 min | 1-3 min |
| **Memory Used** | 800-1500 MB | 200-400 MB |
| **Records Processed** | 10K-50K | 100-1000 |
| **Cost per Run** | $0.50-$1.00 | $0.01-$0.02 |
| **Frequency** | One-time | Daily |
| **Data Volume** | Full historical | Incremental |

## 📚 Referencias

- **Pricing**: [Amazon Bedrock Pricing](https://aws.amazon.com/bedrock/pricing/)
- **Token Calculation**: LENGTH(text) / 4 (approximation)
- **Input Cost**: $0.003 per 1K tokens
- **Output Cost**: $0.015 per 1K tokens

---

**Versión**: 2.0  
**Última actualización**: 2025-01-13  
**Autor**: Data Engineering Team
