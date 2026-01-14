# 🚀 Cat Prod Normalize - Multi-Stack Data Pipeline

Sistema ETL completo para el procesamiento automatizado de conversaciones del chatbot **Catia**, implementado con AWS CDK como pipeline de datos empresarial.

Este proyecto implementa un **sistema de análisis multi-fuente** para conversaciones del chatbot Catia, con dos pipelines independientes: uno para análisis de conversaciones y feedback, y otro para análisis de costos de tokens de Amazon Bedrock Claude Sonnet 3.5.

### 🎯 **Objetivos del Sistema**
- **Pipeline ETL Principal**: Conversaciones DynamoDB → S3 → Athena (Stacks 1-2)
- **Pipeline Tokens Multi-Ambiente**: Análisis de costos Claude con procesamiento histórico y consolidado (Stack 3)
- **Análisis de Costos Bedrock**: Cálculo de tokens y estimaciones de costos AWS
- **Optimización de Datos**: Conversión CSV → Parquet para consultas eficientes
- **Escalabilidad**: Arquitectura serverless multi-stack independiente
- **Monitoreo**: Tags detallados para Cost Explorer y billing por componente

## 🏗️ Arquitectura Multi-Stack Independiente

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        🔄 PIPELINE PRINCIPAL (Stacks 1-2)                   │
│                           Análisis de Conversaciones                          │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ DynamoDB-1  │───▶│ Lambda ETL-1│───▶│   S3 CSV    │───▶│ Glue ETL-2  │
│Conversations│    │(Normalize)  │    │ Raw Reports │    │(Transform)  │
│    Table    │    │             │    │             │    │             │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                           │                                        │
                           ▼                                        ▼
                   ┌─────────────┐                          ┌─────────────┐
                   │EventBridge  │                          │ S3 Parquet  │───▶ Athena
                   │ Scheduler   │                          │Optimized DB │     Analytics
                   └─────────────┘                          └─────────────┘
                                                                    │
                                                                    ▼
                                                            ┌─────────────┐
                                                            │Glue Crawler │
                                                            │Auto-Schema  │
                                                            └─────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                    🪙 PIPELINE TOKENS (Stack 3)                             │
│          Análisis de Tokens con Procesamiento Dual (Multi-Ambiente)         │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────┐         ┌──────────────────┐
│  DynamoDB    │         │   Lambda 1       │        ┌─────────────┐
│  Old Table   │────────▶│   Archival       │───────▶│ S3 Bucket   │
│ (Historical) │         │  (One-time)      │        │ historical/ │
└──────────────┘         └──────────────────┘        └─────────────┘
                                                             │
┌──────────────┐         ┌──────────────────┐               │
│  DynamoDB    │         │   Lambda 2       │               ▼
│  New Table   │────────▶│  Consolidated    │        ┌─────────────┐
│  (Current)   │         │   (Daily)        │───────▶│ S3 Bucket   │───▶ Athena
└──────────────┘         └──────────────────┘        │consolidated/│     View
                                 │                    └─────────────┘
                                 ▼
                         ┌──────────────────┐
                         │  EventBridge     │
                         │ (Test: Disabled) │
                         │ (Prod: Daily)    │
                         └──────────────────┘
```

## 📁 Estructura del Proyecto

```
cat-prod-normalize/
├── 📓 notebook/                                    # Código origen de referencia
│   ├── cat-prod-normalize-data.ipynb               # Notebook original
│   └── cat_prod_normalize_script.py                # Script convertido
├── 🐍 lambda/                                      # Funciones Lambda multi-ETL
│   ├── README.md                                   # Documentación ETL específica
│   ├── etl-process1/                               # 🔄 ETL-1: Extracción y normalización
│   │   ├── lambda_function.py                      # Core: DynamoDB → CSV
│   │   └── requirements.txt                        # pandas, boto3, numpy
│   ├── etl-process2/                               # 🔄 ETL-2: Transformación a Parquet
│   │   ├── glue_job_script.py                      # Glue: CSV → Parquet + tokens
│   │   └── requirements.txt                        # tiktoken, pyspark
│   └── tokens-process/                             # 💰 Análisis de tokens (Dual Lambda)
│       ├── README.md                               # 📖 Documentación detallada
│       ├── lambda-tokens-archival-processing.py    # Lambda 1: Procesar tabla antigua
│       ├── tokens_lambda.py                        # Lambda 2: Consolidar datos
│       └── requirements.txt                        # pandas, boto3
├── 📚 lib/                                         # Definiciones CDK (3 stacks)
│   ├── configs/                                    # 🆕 Configuraciones por ambiente
│   │   ├── tokens-config.interface.ts              # TypeScript interface
│   │   ├── test-tokens.config.ts                   # Config para TEST
│   │   └── prod-tokens.config.ts                   # Config para PROD
│   ├── constructs/                                 # Componentes reutilizables
│   │   ├── athena-construct.ts                     # WorkGroup Athena
│   │   ├── catalog-construct.ts                    # Glue Database + Crawler
│   │   ├── orchestrator-construct.ts               # EventBridge automation
│   │   └── transform-job-construct.ts              # Glue Job ETL-2
│   └── stacks/                                     # 🏗️ 3 Stacks principales
│       ├── cat-prod-normalize-stack.ts             # Stack 1: ETL-1 (Lambda)
│       ├── cad-prod-etl-stack.ts                   # Stack 2: ETL-2 (Glue)
│       └── cat-prod-tokens-stack.ts                # Stack 3: Tokens (Dual Lambda)
├── 🎯 bin/                                         # Punto de entrada
│   └── cat-prod-normalize.ts                       # App CDK multi-stack multi-env
├── ⚙️ config/                                      # Configuración centralizada
│   ├── accountConfig.json                          # Cuenta AWS (081899001252)
│   ├── config.json                                 # Namespace (cat-prod)
│   └── tags.json                                   # Tags estándar (P0260)
├── 🧪 test/                                        # Tests unitarios
│   └── cat-prod-normalize.test.ts                  # Tests CDK
└── 📋 archivos raíz
    ├── README.md                                   # Este archivo
    ├── package.json                                # Dependencias Node.js
    ├── cdk.json                                    # Configuración CDK
    ├── tsconfig.json                               # TypeScript config
    └── test_token_functions.py                     # Test tokens local
```

## 🏭 Stacks y Recursos Desplegados

### **🔄 Stack 1: `cat-prod-normalize-stack` (ETL-1)**
**Propósito**: Extracción y normalización desde DynamoDB

| Recurso | Nombre | Descripción |
|---------|---------|-------------|
| 🐍 **Lambda** | `cat-prod-lambda-normalize` | ETL-1: DynamoDB → CSV (12 columnas) |
| 📦 **S3 Bucket** | `cat-prod-normalize-reports` | Data Lake central |
| 🔐 **IAM Role** | `CatProdNormalizeETLLambdaRole` | Permisos DynamoDB + S3 |
| ⏰ **EventBridge** | `cat-prod-daily-etl-schedule` | Trigger diario 11:30 PM COL |
| 📊 **Layer** | `AWSSDKPandas-Python39:13` | Dependencias (pandas, boto3) |

### **🔄 Stack 2: `cat-prod-etl2-stack` (ETL-2)**
**Propósito**: Transformación a formato analítico optimizado

| Recurso | Nombre | Descripción |
|---------|---------|-------------|
| ⚡ **Glue Job** | `cat-prod-etl2-parquet` | ETL-2: CSV → Parquet + tokens |
| 🕷️ **Glue Crawler** | `curated-crawler` | Auto-detección de esquemas |
| 🗄️ **Glue Database** | `cat_prod_analytics_db` | Catálogo de metadatos |
| 🔍 **Athena WorkGroup** | `wg-cat-prod-analytics` | Consultas SQL optimizadas |
| 🔐 **IAM Roles** | Multiple | Permisos Glue + S3 + EventBridge |
| 🎯 **EventBridge** | `S3 Object Created` | Trigger automático ETL-2 |

### **💰 Stack 3: `cat-{env}-tokens-stack` (Análisis Tokens - Multi-Ambiente)**
**Propósito**: Análisis dual de tokens con procesamiento histórico y consolidado

| Recurso | Nombre | Descripción |
|---------|---------|-------------|
| 🐍 **Lambda 1** | `cat-{env}-lambda-tokens-archival` | Procesa tabla antigua (one-time) |
| 🐍 **Lambda 2** | `cat-{env}-lambda-tokens-consolidated` | Procesa nueva tabla + consolida (daily) |
| 📊 **Layer** | `cat-{env}-pandas-numpy-layer` | pandas, numpy, boto3 (Python 3.11) |
| 🔐 **IAM Roles** | 2 roles independientes | Permisos específicos por Lambda |
| ⏰ **EventBridge** | `{env}-cat-daily-tokens-schedule` | Test: Disabled, Prod: Daily |
| 🗄️ **Data Sources** | Old + New DynamoDB Tables | Procesamiento dual |
| 📂 **S3 Outputs** | `historical/` + `consolidated/` | Datos históricos + consolidados |

**Configuración por Ambiente**:
- **Test**: Schedule deshabilitado, ejecución manual, 1GB RAM
- **Prod**: Schedule diario 4:30 AM UTC, 2GB RAM, retry automático

### **🎯 Fase 1: ETL-1 (Lambda Normalize)**
1. **Trigger**: EventBridge Schedule `cron(30 4 * * ? *)` (UTC)
2. **Fuente**: DynamoDB Nadia 2
3. **Procesamiento**: 
   - Normalización de usuarios únicos por `user_id`
   - Extracción de preguntas desde JSON `conversation_history`
   - Clasificación de feedback: `like/dislike/mixed`
   - Merge de tablas conversations + feedback
4. **Salida**: `s3://cat-prod-normalize-reports/reports/etl-process1/data_YYYYMMDD.csv`

### **🎯 Fase 2: ETL-2 (Glue Transform)**
1. **Trigger**: S3 Event `ObjectCreated` en `/etl-process1/`
2. **Motor**: Glue Job con Spark (2 workers G.1X)
3. **Procesamiento**:
   - Lectura CSV más reciente con PySpark
   - Conversión de tipos de datos optimizada
   - **Cálculo de tokens** con biblioteca `tiktoken`
   - Generación de archivo Parquet único
4. **Salida**: `s3://cat-prod-normalize-reports/reports/etl-process2/data.parquet`

### **🎯 Fase 3: Catalogación Automática**
1. **Trigger**: Glue Job State Change → `SUCCEEDED`
2. **Acción**: Crawler escanea `/etl-process2/data.parquet/`
3. **Resultado**: Schema actualizado en `cat_prod_analytics_db`
4. **Disponibilidad**: Tabla lista para consultas Athena

### **🎯 Fase 4: Análisis de Tokens (Dual Lambda - Multi-Ambiente)**

**Arquitectura de Dos Fases**:

**Fase 4.1: Procesamiento Histórico (Lambda Archival - ONE-TIME)**
1. **Ejecución**: Manual, una sola vez por ambiente
2. **Fuente**: Tabla DynamoDB antigua (datos históricos pre-migración)
3. **Lógica**: Algoritmo de extracción original (compatible con formato antiguo)
4. **Procesamiento**:
   - Lee tabla antigua completa
   - Filtra por rango de fechas configurado (ej: 2025-08-04 a 2025-12-31)
   - Calcula tokens: `LENGTH(text) / 4`
   - Genera estadísticas de costos
5. **Salida**: `s3://{env}/archives/tokens-analysis/tokens_analysis_old_table.csv`
6. **Config**: Timeout 15min, Memory 1-2GB (test/prod)

**Fase 4.2: Procesamiento Consolidado (Lambda Consolidated - DAILY)**
1. **Trigger**: EventBridge Schedule 
2. **Fuente**: Tabla DynamoDB nueva (datos actuales post-migración)
3. **Lógica**: Algoritmo v2 con soporte para `toolUse/toolResult` (nuevo formato)
4. **Procesamiento**:
   - Lee tabla nueva (datos desde fecha de migración)
   - Aplica lógica mejorada de tokens
   - Lee datos históricos desde S3
   - **Consolida** ambas fuentes
   - Genera CSV único con columna `source` (old_table/new_table)
   - Actualiza vista Athena automáticamente
5. **Salida**: `s3://cat-{env}-normalize-reports/tokens-analysis/tokens_analysis_consolidated.csv`
6. **Config**: Timeout 5min, Memory 512MB
7. **Schedule**: 
   - Test: Deshabilitado (ejecución manual)
   - Prod: Habilitado (diario 11:30 PM Colombia)

**Configuración por Ambiente** (TypeScript):
```typescript
// lib/configs/test-tokens.config.ts
export const testTokensConfig: TokensConfig = {
  environment: 'test',
  oldDynamoTableName: 'old-test-table',
  newDynamoTableName: 'new-test-table',
  outputBucket: 'cat-test-normalize-reports',
  schedule: { enabled: false },  // Manual only
  // ... más configuraciones
};

// lib/configs/prod-tokens.config.ts
export const prodTokensConfig: TokensConfig = {
  environment: 'prod',
  oldDynamoTableName: 'BedrockChatStack-Old',
  newDynamoTableName: 'BedrockChatStack-New',
  outputBucket: 'cat-prod-normalize-reports',
  schedule: { enabled: true, cronExpression: 'cron(30 4 * * ? *)' },
  // ... más configuraciones
};
```

> **📝 Nota**: Stack 3 usa configuración TypeScript multi-ambiente. Ver `lambda/tokens-process/README.md` para detalles técnicos.

## 📊 Esquema de Datos Final

### **🗂️ Columnas del Dataset Principal (12 campos)**

| # | Columna | Tipo | Descripción | Origen |
|---|---------|------|-------------|--------|
| 1 | `usuario_id` | String | ID único del usuario | DynamoDB |
| 2 | `nombre` | String | Nombre completo del usuario | DynamoDB |
| 3 | `email` | String | Email del usuario | DynamoDB |
| 4 | `fecha_primera_conversacion` | String | Primera interacción con Catia | DynamoDB |
| 5 | `fecha_ultima_conversacion` | String | Última interacción registrada | DynamoDB |
| 6 | `numero_conversaciones` | Integer | Total de conversaciones | DynamoDB |
| 7 | `lista_preguntas` | String | Array JSON de preguntas completas | DynamoDB (JSON) |
| 8 | `feedback_likes` | Integer | Total de "Me gusta" | DynamoDB |
| 9 | `feedback_dislikes` | Integer | Total de "No me gusta" | DynamoDB |
| 10 | `feedback_ultima_respuesta` | String | Clasificación: like/dislike/mixed | Calculado |
| 11 | `tokens_total` | Integer | Suma tokens conversaciones | Calculado (tiktoken) |
| 12 | `costo_estimado_usd` | Float | Costo AWS estimado | Calculado |

### **🪙 Columnas del Dataset de Tokens Consolidado**

| # | Columna | Tipo | Descripción |
|---|---------|------|-------------|
| 1 | `create_date` | TIMESTAMP | Fecha/hora de creación |
| 2 | `input_token` | INT | Tokens de entrada (prompt) |
| 3 | `output_token` | INT | Tokens de salida (respuesta) |
| 4 | `precio_token_input` | DECIMAL(10,6) | Costo tokens entrada |
| 5 | `precio_token_output` | DECIMAL(10,6) | Costo tokens salida |
| 6 | `total_price` | DECIMAL(10,6) | Costo total conversación |
| 7 | `pk` | STRING | Partition Key DynamoDB |
| 8 | `sk` | STRING | Sort Key DynamoDB |
| 9 | `source` | STRING | Origen: `old_table` / `new_table` |

## 🚀 Deployment

### **📦 Prerequisitos**
```bash
npm install
npm run build
```

### **🌍 Deploy Multi-Ambiente (Stack 3 - Tokens)**

```bash
# Deploy a TEST
cdk deploy cat-test-tokens-stack -c environment=test

# Deploy a PROD
cdk deploy cat-prod-tokens-stack -c environment=prod

# Deploy todos los stacks (incluyendo Normalize y ETL)
cdk deploy --all -c environment=prod
```

### **🔧 Configuración de Variables**

**Lambda ETL-1** (Stack 1):
```bash
S3_BUCKET_NAME=cat-prod-normalize-reports
OUTPUT_PREFIX=reports/etl-process1/
DYNAMODB_TABLE_NAME=BedrockChatStack-DatabaseConversationTable03F3FD7A-VCTDHISEE1NF
PROJECT_ID=P0260
ENVIRONMENT=PROD
CLIENT=CAT
```

**Lambda Tokens Archival** (Stack 3 - Lambda 1):
```bash
OLD_DYNAMODB_TABLE_NAME=BedrockChatStack-Old-Table
S3_BUCKET_NAME=cat-{env}-normalize-reports
S3_OLD_DATA_PREFIX=archival/tokens-analysis/
FILTER_DATE_START=2025-08-04  # Configurable por ambiente
FILTER_DATE_END=2025-11-30
ENVIRONMENT=TEST|PROD
```

**Lambda Tokens Consolidated** (Stack 3 - Lambda 2):
```bash
DYNAMODB_TABLE_NAME=BedrockChatStack-New-Table
S3_BUCKET_NAME=cat-{env}-normalize-reports
S3_OUTPUT_PREFIX=tokens-analysis/
S3_OLD_DATA_PREFIX=archival/tokens-analysis/
ATHENA_DATABASE=cat_{env}_analytics_db
ATHENA_WORKGROUP=wg-cat-{env}-analytics
FILTER_DATE_START=2026-01-01  # Fecha de migración
ENVIRONMENT=TEST|PROD
```

**Glue Job ETL-2**:
```bash
--INPUT_PREFIX=reports/etl-process1/
--OUTPUT_PREFIX=reports/etl-process2/
--BUCKET_NAME=cat-prod-normalize-reports
```

### **📊 Configuración de Horarios**

```typescript
// EventBridge Schedule - ETL-1 y ETL-2
schedule: events.Schedule.expression('cron(30 4 * * ? *)') // 11:30 PM Colombia

// EventBridge Schedule - Tokens (Stack 3)
// Test: Disabled (manual execution)
// Prod: cron(30 4 * * ? *) // 11:30 PM Colombia
```

### **🏷️ Sistema de Tags para Cost Explorer**

| Tag | Valor | Propósito |
|-----|-------|----------|
| `Project` | `CAT-PROD-NORMALIZE`, `CAT-TOKENS-ANALYSIS` | Identificación proyecto |
| `Environment` | `TEST`, `PROD` | Ambiente |
| `ETLComponent` | `ETL-1`, `ETL-2`, `TOKENS-ARCHIVAL`, `TOKENS-CONSOLIDATED` | Componente |
| `DataSource` | `DynamoDB-Old`, `DynamoDB-New` | Fuente datos |

## 🧪 Testing y Validación

### **🔍 Tests Locales**

```bash
# Tests unitarios CDK
npm run test

# Test función tokens archival local
cd lambda/tokens-process
python lambda-tokens-archival-processing.py

# Test función tokens consolidated local
python tokens_lambda.py

# Test Lambda ETL-1 local
cd lambda/etl-process1
python -c "
import lambda_function
result = lambda_function.lambda_handler({}, {})
print(result)
"
```

### **📊 Monitoreo en Producción**

#### **CloudWatch Logs - Stack 3 (Tokens)**
```bash
# Logs Lambda Archival
aws logs tail /aws/lambda/cat-prod-lambda-tokens-archival-processing --follow

# Logs Lambda Consolidated
aws logs tail /aws/lambda/cat-prod-lambda-tokens --follow

# Logs ETL-1 Lambda
aws logs tail /aws/lambda/cat-prod-lambda-normalize --follow

# Logs ETL-2 Glue
aws logs tail /aws-glue/jobs/cat-prod-etl2-parquet --follow
```

#### **Métricas Clave por Stack**
| Métrica | ETL-1 Lambda | ETL-2 Glue | Tokens Archival | Tokens Consolidated |
|---------|--------------|------------|-----------------|---------------------|
| **Duration** | < 15 min | < 10 min | < 15 min | < 5 min |
| **Memory** | < 1024 MB | N/A | 1-2 GB | 512 MB |
| **Frequency** | Daily | Auto (S3) | One-time | Daily (prod) |
| **Cost/día** | ~$0.10 | ~$0.50 | N/A | ~$0.02 |
| **Data Source** | DynamoDB Catia | S3 CSV | DynamoDB Old | DynamoDB New |

#### **Validación de Datos**
```sql
-- Athena: Validar ETL-2 output
SELECT 
    COUNT(*) as total_usuarios,
    MIN(fecha_primera_conversacion) as fecha_min,
    MAX(fecha_primera_conversacion) as fecha_max,
    AVG(numero_conversaciones) as promedio_conversaciones
FROM cat_prod_analytics_db.data;

-- Athena: Validar tokens consolidados por origen
SELECT 
    source,
    COUNT(*) as conversaciones,
    SUM(input_token) as tokens_entrada,
    SUM(output_token) as tokens_salida,
    SUM(total_price) as costo_total_usd
FROM cat_{env}_analytics_db.tokens_table
GROUP BY source;

-- Athena: Análisis diario consolidado
SELECT 
    DATE(create_date) as fecha,
    source,
    COUNT(*) as registros,
    SUM(total_price) as costo_diario
FROM cat_prod_analytics_db.tokens_table
GROUP BY DATE(create_date), source
ORDER BY fecha DESC, source;
```

## ⚙️ Comandos de Gestión

### **📋 Comandos CDK Principales**

| Comando | Descripción | Uso |
|---------|-------------|-----|
| `npm run build` | Compilar TypeScript | Antes de deploy |
| `npm run watch` | Compilación automática | Desarrollo |
| `npx cdk synth` | Generar CloudFormation | Validación |
| `npx cdk deploy --all` | Desplegar todos los stacks | Deploy completo |
| `npx cdk diff <stack>` | Ver cambios pendientes | Pre-deploy |
| `npx cdk destroy --all` | Eliminar todos los recursos | Cleanup |

### **🔄 Operaciones por Stack y Ambiente**

```bash
# Deploy selectivo por stack
npx cdk deploy cat-prod-normalize-stack    # Solo ETL-1
npx cdk deploy cat-prod-etl2-stack         # Solo ETL-2  
npx cdk deploy cat-test-tokens-stack -c environment=test   # Tokens TEST
npx cdk deploy cat-prod-tokens-stack -c environment=prod   # Tokens PROD

# Ejecución manual Lambda Archival (one-time)
aws lambda invoke \
  --function-name cat-prod-lambda-tokens-archival-processing \
  --payload '{}' \
  archival-response.json

# Ejecución manual Lambda Consolidated
aws lambda invoke \
  --function-name cat-prod-lambda-tokens \
  --payload '{}' \
  consolidated-response.json

# Trigger manual ETL-1
aws lambda invoke \
  --function-name cat-prod-lambda-normalize \
  --payload '{}' \
  response.json

# Estado del Glue Job
aws glue get-job-runs --job-name cat-prod-etl2-parquet

# Verificar Crawler
aws glue get-crawler --name curated-crawler
```

### **📊 Consultas Athena de Validación**

```sql
-- Verificar estructura tabla tokens consolidada
DESCRIBE tokens_table_consolidated;

-- KPIs consolidados de tokens
SELECT 
    source,
    COUNT(*) as total_conversaciones,
    SUM(input_token + output_token) as total_tokens,
    SUM(total_price) as costo_total_usd,
    AVG(total_price) as costo_promedio
FROM cat_prod_analytics_db.tokens_table
GROUP BY source;

-- Verificar integridad datos ETL-1 → ETL-2
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns 
WHERE table_schema = 'cat_prod_analytics_db'
ORDER BY table_name, ordinal_position;
```

## 📚 Documentación Adicional

- **Tokens Processing**: Ver `lambda/tokens-process/README.md` para detalles técnicos del procesamiento dual
- **Lambda ETL**: Ver `lambda/README.md` para detalles de ETL-1 y ETL-2
- **Configuración Multi-Ambiente**: Ver `lib/configs/` para configuraciones por ambiente

---

**Última actualización**: 2025-01-13  
**Versión Stack 3**: 2.0 (Dual Lambda + Multi-Ambiente)