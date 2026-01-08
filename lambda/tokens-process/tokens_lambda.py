#!/usr/bin/env python3
"""
AWS Lambda CONSOLIDADO para extraer tokens de DynamoDB (tabla nueva)
y consolidar con datos históricos de la tabla antigua

FLUJO:
1. Procesa tabla NUEVA con lógica v2 (toolUse/toolResult)
2. Lee archivo histórico de tabla ANTIGUA desde S3
3. Consolida ambos datasets
4. Genera CSV consolidado y actualiza vista Athena

VERSIÓN: 2.0 - Consolidación de datos históricos y nuevos
"""

import json
import boto3
import pandas as pd
from datetime import datetime, date
from typing import Tuple, Dict, Any, List
import io
import os
from decimal import Decimal

# ==================== CONFIGURACIÓN AWS ====================
# Tabla NUEVA de DynamoDB
DYNAMODB_TABLE_NAME = os.environ.get(
    'DYNAMODB_TABLE_NAME', 
    'cattest4-BedrockChatStack-DatabaseConversationTableV3C1D85773-1PPI6V82M1BMI'  # ACTUALIZAR con nombre de tabla nueva
)

# S3 Configuration
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'cat-test-normalize-reports')
S3_OUTPUT_PREFIX = os.environ.get('S3_OUTPUT_PREFIX', 'tokens-analysis/')
S3_OLD_DATA_PREFIX = os.environ.get('S3_OLD_DATA_PREFIX', 'tokens-analysis/historical/')

# Athena Configuration
ATHENA_DATABASE = os.environ.get('ATHENA_DATABASE', 'cat_test_analytics_db') 
ATHENA_WORKGROUP = os.environ.get('ATHENA_WORKGROUP', 'wg-cat-test-analytics')
ATHENA_OUTPUT_LOCATION = os.environ.get('ATHENA_OUTPUT_LOCATION', f's3://{S3_BUCKET_NAME}/athena/results/')
ATHENA_VIEW_NAME = os.environ.get('ATHENA_VIEW_NAME', 'tokens_usage_analysis')

# Archivo histórico de tabla antigua
OLD_TABLE_CSV_FILENAME = "tokens_analysis_old_table.csv"

# Rango de fechas para datos nuevos (desde fecha de migración en adelante)
FILTER_DATE_START = datetime(2025, 12, 27, 0, 0, 0)  
FILTER_DATE_END = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)
FILTER_TIMESTAMP_START = int(FILTER_DATE_START.timestamp() * 1000)
FILTER_TIMESTAMP_END = int(FILTER_DATE_END.timestamp() * 1000)

# Inicializar clientes AWS
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
athena_client = boto3.client('athena')

# ==================== FUNCIONES AUXILIARES ====================

def calculate_tokens(text: str) -> int:
    """Calcula tokens aproximados: LENGTH(texto) / 4"""
    if not text or not isinstance(text, str):
        return 0
    return max(1, len(text) // 4)

def clean_and_parse_json(json_str: str) -> dict:
    """Limpia y parsea un string JSON"""
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        try:
            cleaned = json_str.replace('\n', '').replace('\r', '')
            return json.loads(cleaned)
        except:
            return None

def deserializar_dynamodb_item(item: dict) -> dict:
    """Deserializa un item de DynamoDB con tipos nativos de Python"""
    try:
        def convert_value(value):
            if isinstance(value, Decimal):
                return float(value) if value % 1 else int(value)
            elif isinstance(value, dict):
                return {k: convert_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [convert_value(v) for v in value]
            return value
        
        return convert_value(item)
    except Exception:
        return item

# ==================== LÓGICA NUEVA (V2) ====================

def extract_tokens_from_messagemap_v2(message_map: dict) -> Tuple[int, int]:
    """
    VERSIÓN 2: Extrae tokens con soporte para toolUse/toolResult
    """
    input_tokens = 0
    output_tokens = 0
    
    try:
        if not message_map or not isinstance(message_map, dict):
            return 1, 1
        
        for node_id, node_data in message_map.items():
            if not isinstance(node_data, dict):
                continue
            
            role = node_data.get('role', '').lower()
            content = node_data.get('content', [])
            
            node_input, node_output = process_node_content(content, role)
            input_tokens += node_input
            output_tokens += node_output
            
            used_chunks = node_data.get('used_chunks')
            if used_chunks and isinstance(used_chunks, list):
                for chunk in used_chunks:
                    if isinstance(chunk, dict):
                        chunk_text = chunk.get('content', '')
                        if chunk_text:
                            input_tokens += calculate_tokens(str(chunk_text))
        
        if input_tokens == 0 and output_tokens == 0:
            return 1, 1
            
        return input_tokens, output_tokens
        
    except Exception as e:
        print(f"Error en extract_tokens_from_messagemap_v2: {str(e)}")
        return 1, 1

def process_node_content(content: Any, parent_role: str) -> Tuple[int, int]:
    """Procesa el contenido de un nodo recursivamente"""
    input_tokens = 0
    output_tokens = 0
    
    if not content:
        return 0, 0
    
    if isinstance(content, list):
        for item in content:
            if isinstance(item, dict):
                item_input, item_output = process_content_item(item, parent_role)
                input_tokens += item_input
                output_tokens += item_output
    elif isinstance(content, dict):
        item_input, item_output = process_content_item(content, parent_role)
        input_tokens += item_input
        output_tokens += item_output
    elif isinstance(content, str):
        tokens = calculate_tokens(content)
        if parent_role in ['user', 'system', 'instruction']:
            input_tokens += tokens
        elif parent_role in ['assistant', 'bot']:
            output_tokens += tokens
    
    return input_tokens, output_tokens

def process_content_item(item: dict, parent_role: str) -> Tuple[int, int]:
    """Procesa un item individual de contenido por tipo"""
    input_tokens = 0
    output_tokens = 0
    
    content_type = item.get('content_type', 'text')
    
    if content_type == 'text':
        body = item.get('body', '')
        if isinstance(body, str) and body:
            tokens = calculate_tokens(body)
            if parent_role in ['user', 'system', 'instruction']:
                input_tokens += tokens
            elif parent_role in ['assistant', 'bot']:
                output_tokens += tokens
    
    elif content_type == 'toolUse':
        body = item.get('body', {})
        if isinstance(body, dict):
            tool_input = body.get('input', {})
            if tool_input:
                tool_text = json.dumps(tool_input, ensure_ascii=False)
                output_tokens += calculate_tokens(tool_text)
    
    elif content_type == 'toolResult':
        body = item.get('body', {})
        if isinstance(body, dict):
            result_content = body.get('content', [])
            if isinstance(result_content, list):
                for result_item in result_content:
                    if isinstance(result_item, dict):
                        json_content = result_item.get('json', {})
                        if isinstance(json_content, dict):
                            content_text = json_content.get('content', '')
                            if content_text:
                                input_tokens += calculate_tokens(str(content_text))
    
    if 'role' in item and 'content' in item:
        nested_role = item.get('role', '').lower()
        nested_content = item.get('content', [])
        nested_input, nested_output = process_node_content(nested_content, nested_role)
        input_tokens += nested_input
        output_tokens += nested_output
    
    return input_tokens, output_tokens

# ==================== PROCESAMIENTO DYNAMODB (TABLA NUEVA) ====================

def extraer_datos_dynamodb() -> List[Dict]:
    """Extrae datos de la tabla NUEVA de DynamoDB"""
    try:
        table = dynamodb.Table(DYNAMODB_TABLE_NAME)
        response = table.scan()
        items = response['Items']
        
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])
        
        return items
        
    except Exception as e:
        print(f"Error extrayendo datos de DynamoDB: {str(e)}")
        raise e

def procesar_tokens_dynamodb(raw_data: List[Dict]) -> Dict:
    """Procesa datos de tabla NUEVA con lógica v2"""
    results = []
    processed_count = 0
    filtered_count = 0
    error_count = 0
    
    for item_num, item in enumerate(raw_data, 1):
        try:
            create_time = item.get('CreateTime')
            create_date_str = ""
            
            if create_time:
                try:
                    if isinstance(create_time, Decimal):
                        create_timestamp = int(create_time)
                    elif isinstance(create_time, str):
                        create_timestamp = int(create_time)
                    else:
                        create_timestamp = int(create_time)
                    
                    create_date = datetime.fromtimestamp(create_timestamp / 1000)
                    create_date_str = create_date.strftime('%Y-%m-%d %H:%M:%S')
                    
                    if create_timestamp < FILTER_TIMESTAMP_START or create_timestamp > FILTER_TIMESTAMP_END:
                        filtered_count += 1
                        continue
                        
                except (ValueError, TypeError):
                    create_date_str = "Fecha inválida"
            
            message_map = item.get('MessageMap')
            
            if not message_map:
                input_tokens = 0
                output_tokens = 0
                total_price = item.get('TotalPrice', 0.0)
            else:
                if isinstance(message_map, str):
                    json_data = clean_and_parse_json(message_map)
                    if not json_data and 'ody": ",' in message_map:
                        fixed_message = message_map.replace('ody": ",', 'ody": "",')
                        try:
                            json_data = json.loads(fixed_message)
                        except:
                            pass
                else:
                    json_data = deserializar_dynamodb_item(message_map)
                
                if not json_data:
                    input_tokens = 0
                    output_tokens = 0
                    total_price = item.get('TotalPrice', 0.0)
                else:
                    # Usar lógica V2 para tabla nueva
                    input_tokens, output_tokens = extract_tokens_from_messagemap_v2(json_data)
                    
                    precio_input = round((input_tokens * 0.003) / 1000, 6)
                    precio_output = round((output_tokens * 0.015) / 1000, 6)
                    total_price = round(precio_input + precio_output, 6)
            
            results.append({
                'create_date': create_date_str,
                'input_token': input_tokens,
                'output_token': output_tokens,
                'precio_token_input': round((input_tokens * 0.003) / 1000, 6),
                'precio_token_output': round((output_tokens * 0.015) / 1000, 6),
                'total_price': total_price,
                'pk': item.get('PK', ''),
                'sk': item.get('SK', ''),
                'source': 'new_table'  # Identificador de origen
            })
            
            processed_count += 1
            
        except Exception as e:
            error_count += 1
            print(f"Error procesando item {item_num}: {str(e)}")
            continue
    
    return {
        'data': results,
        'processed_count': processed_count,
        'filtered_count': filtered_count,
        'error_count': error_count
    }

# ==================== CONSOLIDACIÓN DE DATOS ====================

def leer_datos_historicos_s3() -> pd.DataFrame:
    """
    Lee el archivo CSV de datos históricos desde S3
    """
    try:
        s3_key = f"{S3_OLD_DATA_PREFIX}{OLD_TABLE_CSV_FILENAME}"
        
        print(f"Leyendo datos históricos de: s3://{S3_BUCKET_NAME}/{s3_key}")
        
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
        csv_content = response['Body'].read().decode('utf-8')
        
        # Leer CSV en DataFrame
        df = pd.read_csv(io.StringIO(csv_content))
        
        print(f"Datos históricos cargados: {len(df)} registros")
        
        return df
        
    except s3_client.exceptions.NoSuchKey:
        print(f"⚠️  Archivo histórico no encontrado en S3: {s3_key}")
        print("Se continuará solo con datos nuevos")
        return pd.DataFrame()
        
    except Exception as e:
        print(f"⚠️  Error leyendo datos históricos: {str(e)}")
        print("Se continuará solo con datos nuevos")
        return pd.DataFrame()

def consolidar_datos(datos_nuevos: List[Dict], df_historico: pd.DataFrame) -> pd.DataFrame:
    """
    Consolida datos nuevos con datos históricos
    """
    # Convertir datos nuevos a DataFrame
    df_nuevos = pd.DataFrame(datos_nuevos)
    
    print(f"Registros nuevos: {len(df_nuevos)}")
    print(f"Registros históricos: {len(df_historico)}")
    
    # Si no hay datos históricos, retornar solo los nuevos
    if df_historico.empty:
        print("No hay datos históricos, usando solo datos nuevos")
        return df_nuevos
    
    # Asegurar que ambos DataFrames tienen las mismas columnas
    columnas_requeridas = [
        'create_date', 'input_token', 'output_token', 
        'precio_token_input', 'precio_token_output', 'total_price',
        'pk', 'sk', 'source'
    ]
    
    # Agregar columnas faltantes si es necesario
    for col in columnas_requeridas:
        if col not in df_historico.columns:
            df_historico[col] = ''
        if col not in df_nuevos.columns:
            df_nuevos[col] = ''
    
    # Concatenar ambos DataFrames
    df_consolidado = pd.concat([df_historico, df_nuevos], ignore_index=True)
    
    # Ordenar por fecha
    df_consolidado = df_consolidado.sort_values('create_date', ascending=False)
    
    # Eliminar duplicados si existen (basado en pk y sk)
    df_consolidado = df_consolidado.drop_duplicates(subset=['pk', 'sk'], keep='first')
    
    print(f"Total registros consolidados: {len(df_consolidado)}")
    
    return df_consolidado

# ==================== GENERACIÓN CSV Y S3 ====================

def generar_y_subir_csv_consolidado(df_consolidado: pd.DataFrame) -> str:
    """
    Genera CSV consolidado y lo sube a S3
    """
    try:
        if df_consolidado.empty:
            print("No hay datos para generar CSV")
            empty_record = {
                'create_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'input_token': 0, 'output_token': 0,
                'precio_token_input': 0.0, 'precio_token_output': 0.0,
                'total_price': 0.0, 'pk': 'sin_datos', 'sk': 'sin_datos',
                'source': 'empty'
            }
            df_consolidado = pd.DataFrame([empty_record])
        
        # Nombre fijo para sobrescritura
        filename = "tokens_analysis_consolidated.csv"
        
        # Convertir DataFrame a CSV
        csv_buffer = io.StringIO()
        df_consolidado.to_csv(csv_buffer, index=False, encoding='utf-8')
        csv_content = csv_buffer.getvalue()
        
        # Subir a S3
        s3_key = f"{S3_OUTPUT_PREFIX}{filename}"
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=csv_content.encode('utf-8'),
            ContentType='text/csv'
        )
        
        s3_url = f"s3://{S3_BUCKET_NAME}/{s3_key}"
        print(f"Archivo CSV consolidado subido a: {s3_url}")
        
        return s3_url
        
    except Exception as e:
        print(f"Error generando/subiendo CSV: {str(e)}")
        raise e

# ==================== ESTADÍSTICAS ====================

def calcular_estadisticas_finales(df: pd.DataFrame) -> Dict:
    """Calcula estadísticas del DataFrame consolidado"""
    
    if df.empty:
        return {
            'total_records': 0, 'total_input_tokens': 0,
            'total_output_tokens': 0, 'total_tokens': 0,
            'total_input_cost': 0, 'total_output_cost': 0,
            'total_cost': 0, 'average_cost_per_record': 0,
            'average_input_tokens': 0, 'average_output_tokens': 0,
            'old_table_records': 0, 'new_table_records': 0
        }
    
    total_input_tokens = df['input_token'].sum()
    total_output_tokens = df['output_token'].sum()
    total_cost = df['total_price'].sum()
    
    total_input_cost = round(total_input_tokens * 0.003 / 1000, 6)
    total_output_cost = round(total_output_tokens * 0.015 / 1000, 6)
    
    # Contar registros por fuente
    old_count = len(df[df['source'] == 'old_table']) if 'source' in df.columns else 0
    new_count = len(df[df['source'] == 'new_table']) if 'source' in df.columns else 0
    
    return {
        'total_records': len(df),
        'total_input_tokens': int(total_input_tokens),
        'total_output_tokens': int(total_output_tokens),
        'total_tokens': int(total_input_tokens + total_output_tokens),
        'total_input_cost': total_input_cost,
        'total_output_cost': total_output_cost,
        'total_cost': round(total_cost, 6),
        'average_cost_per_record': round(total_cost / len(df), 6),
        'average_input_tokens': round(total_input_tokens / len(df), 2),
        'average_output_tokens': round(total_output_tokens / len(df), 2),
        'old_table_records': old_count,
        'new_table_records': new_count
    }

# ==================== ATHENA ====================

def actualizar_vista_athena() -> str:
    """Actualiza vista Athena con datos consolidados"""
    try:
        query = f"""
        CREATE OR REPLACE VIEW {ATHENA_VIEW_NAME} AS
        SELECT
            create_date,
            input_token AS "token pregunta",
            output_token AS "token respuesta",
            input_token + output_token AS "total tokens",
            precio_token_input AS "precio total pregunta",
            precio_token_output AS "precio total respuesta",
            total_price AS "precio total",
            source AS "origen datos"
        FROM tokens_table
        WHERE input_token > 0 OR output_token > 0
        ORDER BY create_date DESC;
        """
        
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': ATHENA_DATABASE},
            ResultConfiguration={'OutputLocation': ATHENA_OUTPUT_LOCATION},
            WorkGroup=ATHENA_WORKGROUP
        )
        
        query_execution_id = response['QueryExecutionId']
        print(f"Vista Athena actualizada: {query_execution_id}")
        return query_execution_id
        
    except Exception as e:
        print(f"Error actualizando vista Athena: {str(e)}")
        return "error"

# ==================== LAMBDA HANDLER ====================

def lambda_handler(event, context):
    """
    Función Lambda principal - CONSOLIDADA
    """
    try:
        print("=" * 60)
        print("PROCESAMIENTO CONSOLIDADO DE TOKENS")
        print("=" * 60)
        print(f"Tabla nueva: {DYNAMODB_TABLE_NAME}")
        print(f"Fecha inicio datos nuevos: {FILTER_DATE_START.strftime('%Y-%m-%d')}")
        print(f"Fecha fin: {FILTER_DATE_END.strftime('%Y-%m-%d')}")
        
        # 1. Procesar tabla NUEVA
        print("\n[1/5] Extrayendo datos de tabla Nadia v.4 ...")
        raw_data_nueva = extraer_datos_dynamodb()
        print(f"Registros extraídos tabla nueva: {len(raw_data_nueva)}")
        
        print("\n[2/5] Procesando tokens tabla nueva (lógica v2)...")
        results_nueva = procesar_tokens_dynamodb(raw_data_nueva)
        print(f"Procesados: {results_nueva['processed_count']}, Filtrados: {results_nueva['filtered_count']}")
        
        # 2. Leer datos históricos
        print("\n[3/5] Leyendo datos históricos de S3...")
        df_historico = leer_datos_historicos_s3()
        
        # 3. Consolidar datos
        print("\n[4/5] Consolidando datos...")
        df_consolidado = consolidar_datos(results_nueva['data'], df_historico)
        
        # 4. Generar CSV consolidado
        print("\n[5/5] Generando CSV consolidado y subiendo a S3...")
        s3_url = generar_y_subir_csv_consolidado(df_consolidado)
        
        # 5. Calcular estadísticas
        stats = calcular_estadisticas_finales(df_consolidado)
        
        # 6. Actualizar Athena
        print("\nActualizando vista Athena...")
        query_id = actualizar_vista_athena()
        
        # Mostrar resumen
        print("\n" + "=" * 60)
        print("RESUMEN CONSOLIDADO")
        print("=" * 60)
        print(f"Registros tabla antigua:  {stats['old_table_records']:,}")
        print(f"Registros tabla nueva:    {stats['new_table_records']:,}")
        print(f"Total registros:          {stats['total_records']:,}")
        print(f"\nTotal input tokens:       {stats['total_input_tokens']:,}")
        print(f"Total output tokens:      {stats['total_output_tokens']:,}")
        print(f"Costo total:              ${stats['total_cost']:.6f} USD")
        print(f"\nArchivo S3:               {s3_url}")
        print("=" * 60)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Procesamiento consolidado completado',
                'statistics': stats,
                'new_table_processed': results_nueva['processed_count'],
                'new_table_filtered': results_nueva['filtered_count'],
                'new_table_errors': results_nueva['error_count'],
                's3_file': s3_url,
                'athena_query_id': query_id,
                'timestamp': datetime.now().isoformat()
            }, default=str)
        }
        
    except Exception as e:
        print(f"Error en lambda_handler: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
        }
