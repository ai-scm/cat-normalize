#!/usr/bin/env python3
"""
AWS Lambda para procesar datos ANTIGUOS de DynamoDB (tabla vieja)
Usa la lógica ORIGINAL de extracción de tokens
Genera archivo CSV y lo sube a S3 para consolidación posterior

FUNCIÓN: ONE-TIME execution (ejecutar manualmente una vez)
PROPÓSITO: Procesar datos históricos de la tabla antigua

CONFIGURACIÓN:
    - Variables de entorno de Lambda
    - Tabla DynamoDB antigua
    - Bucket S3 para almacenar el archivo histórico
"""

import json
import boto3
import pandas as pd
from datetime import datetime
from typing import Tuple, Dict, Any, List
import io
import os
from decimal import Decimal

# ==================== CONFIGURACIÓN ====================
# Tabla ANTIGUA de DynamoDB
OLD_DYNAMODB_TABLE_NAME = os.environ.get(
    'OLD_DYNAMODB_TABLE_NAME', 
    'BedrockChatStack-DatabaseConversationTable03F3FD7A-VCTDHISEE1NF'
)

# S3 para almacenar archivo histórico
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'test-mg-cat-normalize-reports')
S3_OLD_DATA_PREFIX = os.environ.get('S3_OLD_DATA_PREFIX', 'tokens-analysis/historical/')

# Rango de fechas para datos antiguos
FILTER_DATE_START_STR = os.environ.get('FILTER_DATE_START', '2025-08-04')
FILTER_DATE_END_STR = os.environ.get('FILTER_DATE_END', '2025-12-30')

# Convertir strings a datetime
FILTER_DATE_START = datetime.strptime(FILTER_DATE_START_STR, '%Y-%m-%d').replace(hour=0, minute=0, second=0)
FILTER_DATE_END = datetime.strptime(FILTER_DATE_END_STR, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
FILTER_TIMESTAMP_START = int(FILTER_DATE_START.timestamp() * 1000)
FILTER_TIMESTAMP_END = int(FILTER_DATE_END.timestamp() * 1000)

# Inicializar clientes AWS
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

# ==================== FUNCIONES AUXILIARES ====================

def calculate_tokens(text: str) -> int:
    """
    Calcula tokens aproximados: LENGTH(texto) / 4
    """
    if not text or not isinstance(text, str):
        return 0
    return max(1, len(text) // 4)

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

# ==================== LÓGICA ORIGINAL DE TOKENS ====================

def extract_tokens_from_messagemap_OLD(message_map: dict) -> Tuple[int, int]:
    """
    LÓGICA ORIGINAL: Extrae tokens del MessageMap usando formato ANTIGUO
    Esta es la función original sin modificaciones para el nuevo formato
    
    Args:
        message_map: MessageMap en formato antiguo
    
    Returns:
        Tupla (input_tokens, output_tokens)
    """
    input_tokens = 0
    output_tokens = 0
    
    try:
        if not message_map or not isinstance(message_map, dict):
            return 1, 1
        
        # Iterar sobre cada nodo del MessageMap
        for key, value in message_map.items():
            if not isinstance(value, dict):
                continue
                
            role = value.get('role', '').lower()
            content = value.get('content', [])
            used_chunks = value.get('used_chunks', [])
            
            # Procesar used_chunks (entrada)
            if used_chunks and isinstance(used_chunks, list):
                for chunk in used_chunks:
                    if isinstance(chunk, dict):
                        chunk_text = chunk.get('content', '')
                        if chunk_text:
                            input_tokens += calculate_tokens(str(chunk_text))
            
            # Procesar content si es string
            if isinstance(content, str):
                token_count = calculate_tokens(content)
                if role in ['user', 'system', 'instruction', 'used_chunks']:
                    input_tokens += token_count
                elif role in ['assistant', 'bot']:
                    output_tokens += token_count
            
            # Procesar contenido como lista
            elif isinstance(content, list):
                for item in content:
                    if isinstance(item, dict) and 'body' in item:
                        body = item.get('body', '')
                        if isinstance(body, str) and body:
                            token_count = calculate_tokens(body)
                            
                            if role in ['user', 'system', 'instruction', 'used_chunks']:
                                input_tokens += token_count
                            elif role in ['assistant', 'bot']:
                                output_tokens += token_count
            
            # Manejar listas de mensajes (para compatibilidad)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        # Buscar content o body
                        content_item = item.get('content', item.get('body', ''))
                        role_item = item.get('role', '')
                        
                        if isinstance(content_item, str) and content_item:
                            token_count = calculate_tokens(content_item)
                            
                            if role_item in ['user', 'system', 'instruction', 'used_chunks']:
                                input_tokens += token_count
                            elif role_item in ['assistant', 'bot']:
                                output_tokens += token_count
        
        # Si no se encontró ningún token, usar valores mínimos
        if input_tokens == 0 and output_tokens == 0:
            return 1, 1
            
        return input_tokens, output_tokens
        
    except Exception as e:
        print(f"Error en extract_tokens_from_messagemap_OLD: {str(e)}")
        return 1, 1

# ==================== PROCESAMIENTO PRINCIPAL ====================

def extraer_datos_dynamodb_old_table() -> List[Dict]:
    """
    Extrae todos los datos de la tabla ANTIGUA de DynamoDB
    """
    try:
        print(f"Conectando a tabla antigua: {OLD_DYNAMODB_TABLE_NAME}")
        table = dynamodb.Table(OLD_DYNAMODB_TABLE_NAME)
        
        # Usar scan para obtener todos los datos
        response = table.scan()
        items = response['Items']
        
        # Continuar escaneando si hay más datos
        scan_count = 1
        while 'LastEvaluatedKey' in response:
            print(f"Escaneando página {scan_count}... Items hasta ahora: {len(items)}")
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])
            scan_count += 1
        
        print(f"Total de items extraídos: {len(items)}")
        return items
        
    except Exception as e:
        print(f"Error extrayendo datos de DynamoDB antigua: {str(e)}")
        raise e

def procesar_tokens_old_table(raw_data: List[Dict]) -> Dict:
    """
    Procesa los datos de la tabla ANTIGUA usando la lógica ORIGINAL
    """
    results = []
    processed_count = 0
    filtered_count = 0
    error_count = 0
    
    total_items = len(raw_data)
    print(f"\nProcesando {total_items} registros...")
    
    for item_num, item in enumerate(raw_data, 1):
        # Log de progreso cada 100 items
        if item_num % 100 == 0:
            print(f"Procesados {item_num}/{total_items} registros ({(item_num/total_items)*100:.1f}%)...")
        
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
                    
                    # Filtrar por rango de fechas
                    if create_timestamp < FILTER_TIMESTAMP_START or create_timestamp > FILTER_TIMESTAMP_END:
                        filtered_count += 1
                        continue
                        
                except (ValueError, TypeError) as e:
                    print(f"Error procesando fecha en item {item_num}: {str(e)}")
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
                    json_data = clean_and_parse_json(message_map)
                    
                    # Solución de emergencia para errores comunes
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
                    # USAR LÓGICA ORIGINAL
                    input_tokens, output_tokens = extract_tokens_from_messagemap_OLD(json_data)
                    
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
                'sk': item.get('SK', ''),
                'source': 'old_table'  # Identificador de origen
            })
            
            processed_count += 1
            
        except Exception as e:
            error_count += 1
            print(f"Error procesando item {item_num}: {str(e)}")
            continue
    
    print(f"\nProcesamiento completado:")
    print(f"  - Procesados: {processed_count}")
    print(f"  - Filtrados: {filtered_count}")
    print(f"  - Errores: {error_count}")
    
    return {
        'data': results,
        'processed_count': processed_count,
        'filtered_count': filtered_count,
        'error_count': error_count
    }

def generar_y_subir_csv_old(results: Dict) -> str:
    """
    Genera CSV de datos antiguos y lo sube a S3
    Archivo con nombre fijo para referencia posterior
    """
    try:
        if not results.get('data') or len(results['data']) == 0:
            print("No hay datos para generar CSV")
            # Crear registro vacío para evitar errores posteriores
            empty_record = {
                'create_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'input_token': 0,
                'output_token': 0,
                'precio_token_input': 0.0,
                'precio_token_output': 0.0,
                'total_price': 0.0,
                'pk': 'sin_datos',
                'sk': 'sin_datos',
                'source': 'old_table'
            }
            results['data'] = [empty_record]
        
        # Crear DataFrame
        df = pd.DataFrame(results['data'])
        
        # Nombre fijo para el archivo de datos históricos
        filename = "tokens_analysis_old_table.csv"
        
        # Convertir DataFrame a CSV en memoria
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8')
        csv_content = csv_buffer.getvalue()
        
        # Subir a S3
        s3_key = f"{S3_OLD_DATA_PREFIX}{filename}"
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=csv_content.encode('utf-8'),
            ContentType='text/csv'
        )
        
        s3_url = f"s3://{S3_BUCKET_NAME}/{s3_key}"
        print(f"\nArchivo CSV histórico subido a: {s3_url}")
        
        return s3_url
        
    except Exception as e:
        print(f"Error generando/subiendo CSV: {str(e)}")
        raise e

def calcular_estadisticas(data: List[Dict]) -> Dict:
    """
    Calcula estadísticas de los datos procesados
    """
    if not data:
        return {
            'total_records': 0,
            'total_input_tokens': 0,
            'total_output_tokens': 0,
            'total_tokens': 0,
            'total_input_cost': 0,
            'total_output_cost': 0,
            'total_cost': 0,
            'average_cost_per_record': 0,
            'average_input_tokens': 0,
            'average_output_tokens': 0
        }
    
    total_input_tokens = sum(r['input_token'] for r in data if isinstance(r['input_token'], (int, float)))
    total_output_tokens = sum(r['output_token'] for r in data if isinstance(r['output_token'], (int, float)))
    total_cost = sum(r['total_price'] for r in data if isinstance(r['total_price'], (int, float)))
    
    total_input_cost = round(total_input_tokens * 0.003 / 1000, 6)
    total_output_cost = round(total_output_tokens * 0.015 / 1000, 6)
    
    return {
        'total_records': len(data),
        'total_input_tokens': int(total_input_tokens),
        'total_output_tokens': int(total_output_tokens),
        'total_tokens': int(total_input_tokens + total_output_tokens),
        'total_input_cost': total_input_cost,
        'total_output_cost': total_output_cost,
        'total_cost': round(total_cost, 6),
        'average_cost_per_record': round(total_cost / len(data), 6) if data else 0,
        'average_input_tokens': round(total_input_tokens / len(data), 2) if data else 0,
        'average_output_tokens': round(total_output_tokens / len(data), 2) if data else 0
    }

# ==================== LAMBDA HANDLER ====================

def lambda_handler(event, context):
    """
    Función Lambda principal para procesar tabla antigua (ONE-TIME)
    """
    try:
        print("=" * 70)
        print("PROCESAMIENTO DE TABLA ANTIGUA DE DYNAMODB")
        print("=" * 70)
        print(f"Tabla: {OLD_DYNAMODB_TABLE_NAME}")
        print(f"Rango de fechas: {FILTER_DATE_START.strftime('%Y-%m-%d')} a {FILTER_DATE_END.strftime('%Y-%m-%d')}")
        print(f"Bucket S3: {S3_BUCKET_NAME}")
        print(f"Prefijo S3: {S3_OLD_DATA_PREFIX}")
        print("=" * 70)
        
        # 1. Extraer datos de DynamoDB
        print("\n[1/4] Extrayendo datos de DynamoDB...")
        raw_data = extraer_datos_dynamodb_old_table()
        
        if not raw_data:
            return {
                'statusCode': 204,
                'body': json.dumps({
                    'message': 'No se encontraron datos en la tabla antigua',
                    'timestamp': datetime.now().isoformat()
                })
            }
        
        # 2. Procesar tokens
        print("\n[2/4] Procesando tokens con lógica original...")
        results = procesar_tokens_old_table(raw_data)
        
        if not results['data']:
            return {
                'statusCode': 204,
                'body': json.dumps({
                    'message': 'No hay datos después del procesamiento y filtrado',
                    'filtered_count': results['filtered_count'],
                    'timestamp': datetime.now().isoformat()
                })
            }
        
        # 3. Generar y subir CSV
        print("\n[3/4] Generando y subiendo CSV a S3...")
        s3_url = generar_y_subir_csv_old(results)
        
        # 4. Calcular estadísticas
        print("\n[4/4] Calculando estadísticas finales...")
        stats = calcular_estadisticas(results['data'])
        
        # Mostrar resumen en logs
        print("\n" + "=" * 70)
        print("RESUMEN DE PROCESAMIENTO")
        print("=" * 70)
        print(f"Registros procesados:     {stats['total_records']:,}")
        print(f"Registros filtrados:      {results['filtered_count']:,}")
        print(f"Registros con error:      {results['error_count']:,}")
        print(f"\nTokens de entrada:        {stats['total_input_tokens']:,}")
        print(f"Tokens de salida:         {stats['total_output_tokens']:,}")
        print(f"Total de tokens:          {stats['total_tokens']:,}")
        print(f"\nCosto input tokens:       ${stats['total_input_cost']:.6f} USD")
        print(f"Costo output tokens:      ${stats['total_output_cost']:.6f} USD")
        print(f"Costo total:              ${stats['total_cost']:.6f} USD")
        print(f"\nPromedio tokens/registro: {stats['average_input_tokens'] + stats['average_output_tokens']:.2f}")
        print(f"Promedio costo/registro:  ${stats['average_cost_per_record']:.6f} USD")
        print(f"\nArchivo S3:               {s3_url}")
        print("=" * 70)
        print("\n✅ Procesamiento completado exitosamente!")
        
        # Retornar respuesta exitosa
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Procesamiento de tabla antigua completado exitosamente',
                'statistics': stats,
                'processed_count': results['processed_count'],
                'filtered_count': results['filtered_count'],
                'error_count': results['error_count'],
                's3_file': s3_url,
                'date_range': {
                    'start': FILTER_DATE_START.strftime('%Y-%m-%d'),
                    'end': FILTER_DATE_END.strftime('%Y-%m-%d')
                },
                'timestamp': datetime.now().isoformat()
            }, default=str)
        }
        
    except Exception as e:
        print(f"\n❌ Error en el procesamiento: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'error_type': type(e).__name__,
                'timestamp': datetime.now().isoformat()
            })
        }
