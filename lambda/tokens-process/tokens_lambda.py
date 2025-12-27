#!/usr/bin/env python3
"""
AWS Lambda para extraer tokens de DynamoDB y generar CSV con análisis de costos.
VERSIÓN ACTUALIZADA: Soporte para nuevo formato MessageMap con toolUse/toolResult

Basado en la lógica de Athena:
- Input tokens (token_pregunta): contenido de user + system + instruction + used_chunks + toolResult
- Output tokens (token_respuesta): contenido de assistant/bot + toolUse
- Cálculo: LENGTH(texto) / 4 (aproximadamente 4 caracteres por token)
"""

import json
import boto3
import pandas as pd
from datetime import datetime, date
from typing import Tuple, Dict, Any, List
import io
import os
from decimal import Decimal

# Configuración AWS
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME', 'cattest4-BedrockChatStack-DatabaseConversationTableV3C1D85773-1PPI6V82M1BMI')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'test-mg-cat-normalize-reports')
S3_OUTPUT_PREFIX = os.environ.get('S3_OUTPUT_PREFIX', 'tokens-analysis/')
ATHENA_DATABASE = os.environ.get('ATHENA_DATABASE', 'cat_prod_analytics_db') 
ATHENA_WORKGROUP = os.environ.get('ATHENA_WORKGROUP', 'wg-cat-prod-analytics')
ATHENA_OUTPUT_LOCATION = os.environ.get('ATHENA_OUTPUT_LOCATION', f's3://{S3_BUCKET_NAME}/athena/results/')

# Rango de fechas: desde 4 de agosto hasta el día actual (dinámico)
FILTER_DATE_START = datetime(2025, 8, 4, 0, 0, 0)
FILTER_DATE_END = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)
FILTER_TIMESTAMP_START = int(FILTER_DATE_START.timestamp() * 1000)  # Convertir a milisegundos
FILTER_TIMESTAMP_END = int(FILTER_DATE_END.timestamp() * 1000)  # Convertir a milisegundos

# Inicializar clientes AWS
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
athena_client = boto3.client('athena')

def lambda_handler(event, context):
    """
    Función Lambda principal para procesar DynamoDB y generar análisis de tokens
    """
    try:
        print("=== INICIANDO EXTRACCIÓN DE TOKENS ===")
        print(f"Filtro de fecha: desde {FILTER_DATE_START.strftime('%Y-%m-%d %H:%M:%S')} hasta {FILTER_DATE_END.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 1. Extraer datos de DynamoDB
        print("Extrayendo datos de DynamoDB...")
        raw_data = extraer_datos_dynamodb()
        print(f"Registros extraídos: {len(raw_data)}")
        
        if not raw_data:
            return {
                'statusCode': 204,
                'body': json.dumps({
                    'message': 'No se encontraron datos en DynamoDB',
                    'timestamp': datetime.now().isoformat()
                })
            }
        
        # 2. Procesar datos y extraer tokens
        print("Procesando tokens...")
        results = procesar_tokens_dynamodb(raw_data)
        print(f"Registros procesados: {len(results['data'])}")
        print(f"Registros filtrados: {results['filtered_count']}")
        
        # 3. Generar CSV y subir a S3
        print("Generando CSV...")
        s3_url = generar_y_subir_csv(results)
        
        # 4. Generar estadísticas finales
        stats = calcular_estadisticas_finales(results['data'])
        
        # 5. Actualizar vista en Athena
        print("Actualizando vista en Athena...")
        query_id = actualizar_vista_athena()
        print(f"Query ejecutada en Athena ID: {query_id}")
        
        print("=== PROCESAMIENTO COMPLETADO ===")
        print(f"Total input tokens: {stats['total_input_tokens']:,}")
        print(f"Total output tokens: {stats['total_output_tokens']:,}")
        print(f"Costo total input tokens: ${stats['total_input_tokens'] * 0.003 / 1000:.6f} USD")
        print(f"Costo total output tokens: ${stats['total_output_tokens'] * 0.015 / 1000:.6f} USD")
        print(f"Costo total: ${stats['total_cost']:.6f} USD")
        print(f"Archivo S3: {s3_url}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Extracción de tokens completada exitosamente',
                'statistics': stats,
                'filtered_count': results['filtered_count'],
                'processed_count': results['processed_count'],
                'error_count': results['error_count'],
                'total_cost_usd': stats['total_cost'],
                'input_cost_usd': stats['total_input_cost'],
                'output_cost_usd': stats['total_output_cost'],
                's3_file': s3_url,
                'athena_query_id': query_id,
                'timestamp': datetime.now().isoformat()
            }, default=str)
        }
        
    except Exception as e:
        print(f"Error en lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
        }

def extraer_datos_dynamodb() -> List[Dict]:
    """
    Extrae todos los datos relevantes de DynamoDB
    """
    try:
        table = dynamodb.Table(DYNAMODB_TABLE_NAME)
        
        # Usar scan para obtener todos los datos
        response = table.scan()
        items = response['Items']
        
        # Continuar escaneando si hay más datos
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])
        
        return items
        
    except Exception as e:
        print(f"Error extrayendo datos de DynamoDB: {str(e)}")
        raise e

def procesar_tokens_dynamodb(raw_data: List[Dict]) -> Dict:
    """
    Procesa los datos de DynamoDB y extrae tokens
    """
    results = []
    processed_count = 0
    filtered_count = 0
    error_count = 0
    
    for item_num, item in enumerate(raw_data, 1):
        try:
            # Obtener CreateTime y filtrar por fecha
            create_time = item.get('CreateTime')
            create_date_str = ""
            
            if create_time:
                try:
                    # DynamoDB puede devolver Decimal o string
                    if isinstance(create_time, Decimal):
                        create_timestamp = int(create_time)
                    elif isinstance(create_time, str):
                        create_timestamp = int(create_time)
                    else:
                        create_timestamp = int(create_time)
                    
                    # Convertir timestamp a fecha legible
                    create_date = datetime.fromtimestamp(create_timestamp / 1000)
                    create_date_str = create_date.strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Filtrar: solo procesar si está en el rango de fechas
                    if create_timestamp < FILTER_TIMESTAMP_START or create_timestamp > FILTER_TIMESTAMP_END:
                        filtered_count += 1
                        continue
                        
                except (ValueError, TypeError):
                    create_date_str = "Fecha inválida"
            
            # Obtener MessageMap
            message_map = item.get('MessageMap')
            
            if not message_map:
                # Sin MessageMap válido, tokens en 0
                input_tokens = 0
                output_tokens = 0
                total_price = item.get('TotalPrice', 0.0)
            else:
                # Procesar MessageMap (puede ser dict o string)
                if isinstance(message_map, str):
                    # Si es string, parsearlo como JSON
                    json_data = clean_and_parse_json(message_map)
                    
                    # SOLUCIÓN DE EMERGENCIA: Para registros con error en char 99
                    if not json_data and 'ody": ",' in message_map:
                        fixed_message = message_map.replace('ody": ",', 'ody": "",')
                        try:
                            json_data = json.loads(fixed_message)
                        except:
                            pass
                else:
                    # Si ya es dict (formato DynamoDB), usar la deserialización mejorada
                    json_data = deserializar_dynamodb_item(message_map)
                
                if not json_data:
                    input_tokens = 0
                    output_tokens = 0
                    total_price = item.get('TotalPrice', 0.0)
                else:
                    # NUEVA FUNCIÓN: Extraer tokens con soporte para nuevo formato
                    input_tokens, output_tokens = extract_tokens_from_messagemap_v2(json_data)
                    
                    # Calcular precio total
                    precio_input = round((input_tokens * 0.003) / 1000, 6)
                    precio_output = round((output_tokens * 0.015) / 1000, 6)
                    total_price = round(precio_input + precio_output, 6)
            
            # Agregar resultado
            results.append({
                'create_date': create_date_str,
                'input_token': input_tokens,
                'output_token': output_tokens,
                'precio_token_input': round((input_tokens * 0.003) / 1000, 6),
                'precio_token_output': round((output_tokens * 0.015) / 1000, 6),
                'total_price': total_price,
                'pk': item.get('PK', ''),
                'sk': item.get('SK', '')
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

def clean_and_parse_json(json_str: str) -> dict:
    """
    Limpia y parsea un string JSON que puede tener errores de formato
    """
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        try:
            # Intentar limpiar caracteres problemáticos
            cleaned = json_str.replace('\n', '').replace('\r', '')
            return json.loads(cleaned)
        except:
            return None

def deserializar_dynamodb_item(item: dict) -> dict:
    """
    Deserializa un item de DynamoDB con tipos nativos de Python
    """
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

def calculate_tokens(text: str) -> int:
    """
    Calcula tokens aproximados: LENGTH(texto) / 4
    """
    if not text or not isinstance(text, str):
        return 0
    return max(1, len(text) // 4)

def extract_tokens_from_messagemap_v2(message_map: dict) -> Tuple[int, int]:
    """
    VERSIÓN 2: Extrae tokens del MessageMap con soporte para nuevo formato
    Maneja toolUse, toolResult y contenido anidado
    
    Reglas de clasificación:
    - INPUT: user, system, instruction, used_chunks, toolResult (datos para el asistente)
    - OUTPUT: assistant, bot, toolUse (respuestas y solicitudes del asistente)
    """
    input_tokens = 0
    output_tokens = 0
    
    try:
        if not message_map or not isinstance(message_map, dict):
            return 1, 1
        
        # Procesar cada nodo del MessageMap
        for node_id, node_data in message_map.items():
            if not isinstance(node_data, dict):
                continue
            
            role = node_data.get('role', '').lower()
            content = node_data.get('content', [])
            
            # Procesar el contenido del nodo
            node_input, node_output = process_node_content(content, role)
            input_tokens += node_input
            output_tokens += node_output
            
            # NUEVO: Procesar used_chunks si existen
            used_chunks = node_data.get('used_chunks')
            if used_chunks and isinstance(used_chunks, list):
                for chunk in used_chunks:
                    if isinstance(chunk, dict):
                        chunk_text = chunk.get('content', '')
                        if chunk_text:
                            input_tokens += calculate_tokens(str(chunk_text))
        
        # Si no se encontró ningún token, usar valores mínimos
        if input_tokens == 0 and output_tokens == 0:
            return 1, 1
            
        return input_tokens, output_tokens
        
    except Exception as e:
        print(f"Error en extract_tokens_from_messagemap_v2: {str(e)}")
        return 1, 1

def process_node_content(content: Any, parent_role: str) -> Tuple[int, int]:
    """
    Procesa el contenido de un nodo (puede ser lista o dict)
    Maneja recursivamente contenido anidado
    
    Args:
        content: Contenido a procesar (lista o dict)
        parent_role: Rol del nodo padre
    
    Returns:
        Tupla (input_tokens, output_tokens)
    """
    input_tokens = 0
    output_tokens = 0
    
    if not content:
        return 0, 0
    
    # Si el contenido es una lista
    if isinstance(content, list):
        for item in content:
            if isinstance(item, dict):
                item_input, item_output = process_content_item(item, parent_role)
                input_tokens += item_input
                output_tokens += item_output
    
    # Si el contenido es un dict
    elif isinstance(content, dict):
        item_input, item_output = process_content_item(content, parent_role)
        input_tokens += item_input
        output_tokens += item_output
    
    # Si el contenido es string directo
    elif isinstance(content, str):
        tokens = calculate_tokens(content)
        if parent_role in ['user', 'system', 'instruction']:
            input_tokens += tokens
        elif parent_role in ['assistant', 'bot']:
            output_tokens += tokens
    
    return input_tokens, output_tokens

def process_content_item(item: dict, parent_role: str) -> Tuple[int, int]:
    """
    Procesa un item individual de contenido
    Maneja diferentes content_types: text, toolUse, toolResult
    
    Args:
        item: Item de contenido (dict)
        parent_role: Rol del nodo padre
    
    Returns:
        Tupla (input_tokens, output_tokens)
    """
    input_tokens = 0
    output_tokens = 0
    
    content_type = item.get('content_type', 'text')
    
    # CASO 1: Contenido de tipo texto simple
    if content_type == 'text':
        body = item.get('body', '')
        if isinstance(body, str) and body:
            tokens = calculate_tokens(body)
            
            # Clasificar según el rol del padre
            if parent_role in ['user', 'system', 'instruction']:
                input_tokens += tokens
            elif parent_role in ['assistant', 'bot']:
                output_tokens += tokens
    
    # CASO 2: Tool Use (solicitud de herramienta del asistente)
    elif content_type == 'toolUse':
        # toolUse cuenta como OUTPUT (el asistente está solicitando una herramienta)
        body = item.get('body', {})
        if isinstance(body, dict):
            # Extraer texto del input de la herramienta
            tool_input = body.get('input', {})
            if tool_input:
                tool_text = json.dumps(tool_input, ensure_ascii=False)
                output_tokens += calculate_tokens(tool_text)
    
    # CASO 3: Tool Result (resultado de herramienta)
    elif content_type == 'toolResult':
        # toolResult cuenta como INPUT (datos que se proporcionan al asistente)
        body = item.get('body', {})
        if isinstance(body, dict):
            # Procesar el array de contenido dentro del resultado
            result_content = body.get('content', [])
            if isinstance(result_content, list):
                for result_item in result_content:
                    if isinstance(result_item, dict):
                        # Extraer contenido del JSON
                        json_content = result_item.get('json', {})
                        if isinstance(json_content, dict):
                            content_text = json_content.get('content', '')
                            if content_text:
                                input_tokens += calculate_tokens(str(content_text))
    
    # CASO 4: Contenido anidado (mensajes dentro de assistant)
    # En el nuevo formato, los nodos assistant pueden contener arrays de mensajes
    if 'role' in item and 'content' in item:
        nested_role = item.get('role', '').lower()
        nested_content = item.get('content', [])
        
        # Procesar recursivamente el contenido anidado
        nested_input, nested_output = process_node_content(nested_content, nested_role)
        input_tokens += nested_input
        output_tokens += nested_output
    
    return input_tokens, output_tokens

def generar_y_subir_csv(results: Dict) -> str:
    """
    Genera CSV y lo sube a S3 con nombre fijo para sobrescritura diaria
    """
    try:
        # Garantizar que siempre haya datos para el CSV
        if not results.get('data') or len(results['data']) == 0:
            # Crear registro mínimo para evitar error
            empty_record = {
                'create_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'input_token': 0,
                'output_token': 0,
                'precio_token_input': 0.0,
                'precio_token_output': 0.0,
                'total_price': 0.0,
                'pk': 'sin_datos',
                'sk': 'sin_datos'
            }
            results['data'] = [empty_record]
            
        # Crear DataFrame
        df = pd.DataFrame(results['data'])
        
        # Usar nombre fijo para sobrescribir cada día
        filename = "tokens_analysis_latest.csv"
        
        # Convertir DataFrame a CSV en memoria
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8')
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
        print(f"Archivo CSV subido a: {s3_url}")
        
        return s3_url
        
    except Exception as e:
        print(f"Error generando/subiendo CSV: {str(e)}")
        raise e

def calcular_estadisticas_finales(data: List[Dict]) -> Dict:
    """
    Calcula estadísticas finales del procesamiento
    """
    total_input_tokens = sum(
        r['input_token'] for r in data 
        if isinstance(r['input_token'], (int, float))
    )
    total_output_tokens = sum(
        r['output_token'] for r in data 
        if isinstance(r['output_token'], (int, float))
    )
    total_cost = sum(
        r['total_price'] for r in data 
        if isinstance(r['total_price'], (int, float))
    )
    
    # Calcular costos totales de input y output
    total_input_cost = round(total_input_tokens * 0.003 / 1000, 6)
    total_output_cost = round(total_output_tokens * 0.015 / 1000, 6)
    
    return {
        'total_records': len(data),
        'total_input_tokens': total_input_tokens,
        'total_output_tokens': total_output_tokens,
        'total_tokens': total_input_tokens + total_output_tokens,
        'total_input_cost': total_input_cost,
        'total_output_cost': total_output_cost,
        'total_cost': round(total_cost, 6),
        'average_cost_per_record': round(total_cost / len(data), 6) if data else 0,
        'average_input_tokens': round(total_input_tokens / len(data), 2) if data else 0,
        'average_output_tokens': round(total_output_tokens / len(data), 2) if data else 0
    }

def actualizar_vista_athena() -> str:
    """
    Ejecuta una consulta en Athena para actualizar la vista token_usage_analysis
    """
    try:
        # SQL para crear o reemplazar la vista
        query = """
        CREATE OR REPLACE VIEW test_mg_token_usage_analysis AS
        SELECT
            create_date,
            input_token AS "token pregunta",
            output_token AS "token respuesta",
            input_token + output_token AS "total tokens",
            precio_token_input AS "precio total pregunta",
            precio_token_output AS "precio total respuesta",
            total_price AS "precio total"
        FROM tokens_table
        WHERE input_token > 0 OR output_token > 0
        ORDER BY "total tokens" DESC;
        """
        
        # Ejecutar la consulta en Athena
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': ATHENA_DATABASE
            },
            ResultConfiguration={
                'OutputLocation': ATHENA_OUTPUT_LOCATION
            },
            WorkGroup=ATHENA_WORKGROUP
        )
        
        query_execution_id = response['QueryExecutionId']
        print(f"Vista Athena actualizada con ID de ejecución: {query_execution_id}")
        return query_execution_id
        
    except Exception as e:
        print(f"Error actualizando vista en Athena: {str(e)}")
        return "error"