"""
Lambda Function: Feedback Processor for DynamoDB Chatbot Data
Version: 3.1.0 - FINAL - Ultra-aggressive single-line cleaning
"""

import json
import os
import logging
import re
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from io import BytesIO

import boto3
import pandas as pd
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME', 'cat-prod-catia-conversations-table')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'cat-prod-normalize-reports')
S3_OUTPUT_PREFIX = os.environ.get('S3_OUTPUT_PREFIX', 'reports/feedbacks/')
OUTPUT_FORMAT = os.environ.get('OUTPUT_FORMAT', 'parquet')
NAMES_TO_REMOVE = ['roger', 'daniela lalle montaña', 'gerardo pruebas','blend global']

# AWS clients
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
s3_client = boto3.client('s3', region_name='us-east-1')

# Constants
FINAL_COLUMNS = [
    'usuario_id', 'nombre', 'fecha', 'mensaje_usuario', 'mensaje_bot',
    'feedback', 'comentario', 'opcion_respuesta', 'ciudad', 'gerencia',
    'oficinas_asesoras', 'subgerencia', 'nivel_directivo'
]


class TextCleaner:
    """Ultra-aggressive text cleaning to ensure single-line CSV compatibility."""
    
    @staticmethod
    def clean_for_csv(text: str, max_length: int = 10000) -> str:
        """
        ULTRA-AGGRESSIVELY clean text to ensure it's a single line.
        
        Removes ALL:
        - Newlines (\n, \r\n, \r)
        - Tabs (\t)
        - Form feeds (\f)
        - Vertical tabs (\v)
        - Unicode line separators (\u2028, \u2029)
        - Non-printable characters
        - Multiple spaces
        
        Args:
            text: Text to clean
            max_length: Maximum text length
            
        Returns:
            Single-line cleaned text GUARANTEED
        """
        if not text:
            return ''
        
        if not isinstance(text, str):
            text = str(text)
        
        # Step 1: Remove ALL types of line breaks
        cleaned = text.replace('\r\n', ' ')  # Windows CRLF
        cleaned = cleaned.replace('\n', ' ')  # Unix LF
        cleaned = cleaned.replace('\r', ' ')  # Old Mac CR
        
        # Step 2: Remove ALL types of tabs and form feeds
        cleaned = cleaned.replace('\t', ' ')  # Horizontal tab
        cleaned = cleaned.replace('\f', ' ')  # Form feed
        cleaned = cleaned.replace('\v', ' ')  # Vertical tab
        
        # Step 3: Remove Unicode line separators
        cleaned = cleaned.replace('\u2028', ' ')  # Line separator
        cleaned = cleaned.replace('\u2029', ' ')  # Paragraph separator
        cleaned = cleaned.replace('\x85', ' ')   # Next line (NEL)
        
        # Step 4: Remove ALL control characters (ASCII 0-31) except space
        cleaned = ''.join(
            char if (ord(char) >= 32 or char == ' ') else ' '
            for char in cleaned
        )
        
        # Step 5: Replace ANY remaining whitespace with single space using regex
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        # Step 6: Strip leading/trailing whitespace
        cleaned = cleaned.strip()
        
        # Step 7: Final safety check - remove any non-printable characters
        cleaned = ''.join(char if char.isprintable() or char == ' ' else ' ' for char in cleaned)
        
        # Step 8: Final cleanup of multiple spaces
        cleaned = ' '.join(cleaned.split())
        
        # Step 9: Limit length
        if len(cleaned) > max_length:
            cleaned = cleaned[:max_length] + '...'
        
        return cleaned


class DynamoDBExtractor:
    """Handles extraction of data from DynamoDB table."""
    
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.table = dynamodb.Table(table_name)
        logger.info(f"Initialized DynamoDBExtractor for table: {table_name}")
    
    def scan_table(self) -> List[Dict[str, Any]]:
        """Scan entire DynamoDB table with pagination support."""
        logger.info("Starting DynamoDB table scan")
        items = []
        
        try:
            response = self.table.scan()
            items.extend(response.get('Items', []))
            
            while 'LastEvaluatedKey' in response:
                logger.info(f"Fetching next page. Current items: {len(items)}")
                response = self.table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                items.extend(response.get('Items', []))
            
            logger.info(f"Successfully extracted {len(items)} items from DynamoDB")
            return items
            
        except ClientError as e:
            logger.error(f"DynamoDB scan failed: {str(e)}")
            raise


class DataParser:
    """Parses DynamoDB data (boto3.resource already deserializes)."""
    
    @staticmethod
    def extract_user_data(user_data: Any) -> Dict[str, str]:
        """Extract UserData field."""
        default_user_data = {
            'nombre': '', 'ciudad': '', 'gerencia': '',
            'oficinas_asesoras': '', 'subgerencia': '', 'nivel_directivo': ''
        }
        
        if not user_data or not isinstance(user_data, dict):
            return default_user_data
        
        return {
            'nombre': user_data.get('nombre', ''),
            'ciudad': user_data.get('ciudad', ''),
            'gerencia': user_data.get('gerencia', ''),
            'oficinas_asesoras': user_data.get('oficinas_asesoras', ''),
            'subgerencia': user_data.get('subgerencia', ''),
            'nivel_directivo': user_data.get('nivel_directivo', '')
        }
    
    @staticmethod
    def extract_conversation_messages(conversation: Any) -> List[Dict[str, str]]:
        """Extract Conversation field."""
        if not conversation or not isinstance(conversation, list):
            return []
        
        messages = []
        for message in conversation:
            if isinstance(message, dict):
                messages.append({
                    'from': message.get('from', ''),
                    'text': message.get('text', ''),
                    'timestamp': message.get('timestamp', '')
                })
        
        return messages
    
    @staticmethod
    def extract_feedback(feedback: Any) -> Dict[str, str]:
        """Extract Feedback field."""
        default_feedback = {'type': '', 'comment': '', 'option': ''}
        
        if not feedback or not isinstance(feedback, dict):
            return default_feedback
        
        return {
            'type': feedback.get('type', ''),
            'comment': feedback.get('comment', ''),
            'option': feedback.get('option', '')
        }


class ConversationExtractor:
    """Extracts user and bot messages from conversation data."""
    
    @staticmethod
    def get_user_and_bot_messages(messages: List[Dict[str, str]]) -> Tuple[str, str]:
        """Extract and clean user message and bot response."""
        if not messages:
            return ('', '')
        
        user_messages = []
        bot_messages = []
        
        for i, msg in enumerate(messages):
            msg_from = msg.get('from', '').lower()
            msg_text = msg.get('text', '')
            
            if msg_from == 'user':
                user_messages.append({'index': i, 'text': msg_text})
            elif msg_from in ['bot', 'assistant', 'system']:
                bot_messages.append({'index': i, 'text': msg_text})
        
        if not user_messages and not bot_messages:
            return ('', '')
        if not user_messages:
            return ('', bot_messages[-1]['text'])
        if not bot_messages:
            return (user_messages[-1]['text'], '')
        
        last_user = user_messages[-1]
        
        # Find bot response after last user message
        bot_response = None
        for bot_msg in bot_messages:
            if bot_msg['index'] > last_user['index']:
                bot_response = bot_msg['text']
                break
        
        if bot_response is None:
            bot_response = bot_messages[-1]['text']
        
        # Apply AGGRESSIVE cleaning to ensure single line
        user_message = TextCleaner.clean_for_csv(last_user['text'])
        bot_message = TextCleaner.clean_for_csv(bot_response)
        
        return (user_message, bot_message)


class FeedbackProcessor:
    """Associates feedback with corresponding conversation messages."""
    
    @staticmethod
    def parse_timestamp(timestamp_str: str) -> Optional[datetime]:
        """Parse timestamp string to datetime object."""
        if not timestamp_str:
            return None
        
        try:
            return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except (ValueError, AttributeError) as e:
            logger.warning(f"Failed to parse timestamp: {timestamp_str}, error: {str(e)}")
            return None
    
    @staticmethod
    def extract_sk_type_and_timestamp(sk: str) -> Tuple[str, Optional[datetime]]:
        """Extract type and timestamp from Sort Key."""
        if not sk or '#' not in sk:
            return ('UNKNOWN', None)
        
        parts = sk.split('#', 1)
        sk_type = parts[0]
        timestamp_str = parts[1] if len(parts) > 1 else None
        timestamp = FeedbackProcessor.parse_timestamp(timestamp_str) if timestamp_str else None
        
        return (sk_type, timestamp)
    
    @staticmethod
    def associate_feedback_to_conversations(
        conversations: List[Dict[str, Any]],
        feedbacks: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Associate each feedback with closest conversation by timestamp."""
        logger.info(f"Associating {len(feedbacks)} feedbacks with {len(conversations)} conversations")
        
        conversations_by_user: Dict[str, List[Dict[str, Any]]] = {}
        for conv in conversations:
            user_id = conv.get('usuario_id')
            if user_id:
                if user_id not in conversations_by_user:
                    conversations_by_user[user_id] = []
                conversations_by_user[user_id].append(conv)
        
        for user_id in conversations_by_user:
            conversations_by_user[user_id].sort(
                key=lambda x: x.get('timestamp') or datetime.min
            )
        
        associated_feedbacks = []
        no_match_count = 0
        
        for feedback in feedbacks:
            user_id = feedback.get('usuario_id')
            feedback_timestamp = feedback.get('timestamp')
            
            if not user_id or not feedback_timestamp:
                logger.warning(f"Feedback missing user_id or timestamp")
                no_match_count += 1
                continue
            
            user_conversations = conversations_by_user.get(user_id, [])
            
            if not user_conversations:
                logger.warning(f"No conversations found for user: {user_id}")
                feedback['mensaje_usuario'] = ''
                feedback['mensaje_bot'] = ''
                associated_feedbacks.append(feedback)
                no_match_count += 1
                continue
            
            # Find conversation with smallest time difference
            matching_conversation = None
            min_time_diff = None

            #get non-empty user data
            for conv in user_conversations:
                user_data = {'nombre':'', 'ciudad':'', 'gerencia':'', 'oficinas_asesoras':'', 'subgerencia':'', 'nivel_directivo':''}
                for field in user_data.keys():
                    if conv.get(field):
                        user_data[field] = conv.get(field)

            for conv in reversed(user_conversations):
                conv_timestamp = conv.get('timestamp')
                if not conv_timestamp:
                    continue
                
                if conv_timestamp <= feedback_timestamp:
                    matching_conversation = conv
                    if not matching_conversation.get('mensaje_usuario'):
                        continue
                    break
            
            if matching_conversation:
                feedback['mensaje_usuario'] = matching_conversation.get('mensaje_usuario', '')
                feedback['mensaje_bot'] = matching_conversation.get('mensaje_bot', '')
                feedback['nombre'] = user_data.get('nombre', '')
                feedback['ciudad'] = user_data.get('ciudad', '')
                feedback['gerencia'] = user_data.get('gerencia', '')
                feedback['oficinas_asesoras'] = user_data.get('oficinas_asesoras', '')
                feedback['subgerencia'] = user_data.get('subgerencia', '')
                feedback['nivel_directivo'] = user_data.get('nivel_directivo', '')

            else:
                logger.warning(f"No valid conversation found for feedback")
                feedback['mensaje_usuario'] = ''
                feedback['mensaje_bot'] = ''
                no_match_count += 1
            
            associated_feedbacks.append(feedback)
        
        successful_matches = len(associated_feedbacks) - no_match_count
        if successful_matches > 0:
            #avg_time_diff = total_time_diff / successful_matches
            logger.info(
                f"Associated {successful_matches} feedbacks. "
             #   f"Avg time diff: {avg_time_diff:.2f}s. "
                f"No match: {no_match_count}"
            )
        
        return associated_feedbacks


class DataTransformer:
    """Transforms raw data into final dataset structure."""
    
    @staticmethod
    def transform_to_final_dataset(items: List[Dict[str, Any]]) -> pd.DataFrame:
        """Transform DynamoDB items into final dataset."""
        logger.info("Starting data transformation")
        
        conversations = []
        feedbacks = []
        
        for item in items:
            pk = item.get('PK', '')
            sk = item.get('SK', '')
            
            usuario_id = pk.replace('USER#', '') if pk.startswith('USER#') else pk
            sk_type, timestamp = FeedbackProcessor.extract_sk_type_and_timestamp(sk)
            
            if sk_type == 'REGISTER':
                continue
            
            user_data = DataParser.extract_user_data(item.get('UserData'))
            created_at = item.get('CreatedAt', '')
            fecha = FeedbackProcessor.parse_timestamp(created_at)
            fecha_date = fecha.date() if fecha else ''
            
            if sk_type == 'CONVERSATION':
                conversation_messages = DataParser.extract_conversation_messages(
                    item.get('Conversation')
                )
                
                user_message, bot_message = ConversationExtractor.get_user_and_bot_messages(
                    conversation_messages
                )
                
                conversations.append({
                    'usuario_id': usuario_id,
                    'timestamp': timestamp,
                    'fecha': fecha_date,
                    'mensaje_usuario': user_message,
                    'mensaje_bot': bot_message,
                    **user_data
                })
            
            elif sk_type == 'FEEDBACK':
                feedback_data = DataParser.extract_feedback(item.get('Feedback'))
                
                feedbacks.append({
                    'usuario_id': usuario_id,
                    'timestamp': timestamp,
                    'fecha': fecha_date,
                    'feedback': feedback_data.get('type', ''),
                    'comentario': TextCleaner.clean_for_csv(feedback_data.get('comment', '')),
                    'opcion_respuesta': feedback_data.get('option', ''),
                    **user_data
                })
        logger.info(f"Extracted {len(conversations)} conversations and {len(feedbacks)} feedbacks")
        
        associated_feedbacks = FeedbackProcessor.associate_feedback_to_conversations(
            conversations, feedbacks
        )

        associated_feedbacks = [f for f in associated_feedbacks if f.get('nombre', '').strip().lower() not in NAMES_TO_REMOVE]
        
        df = pd.DataFrame(associated_feedbacks)
        
        for col in FINAL_COLUMNS:
            if col not in df.columns:
                df[col] = ''
        
        df = df[FINAL_COLUMNS].fillna('')
        
        logger.info(f"Final dataset created with {len(df)} records")
        return df


class S3Uploader:
    """Handles uploading processed data to S3 in multiple formats."""
    
    @staticmethod
    def upload_dataframe_to_s3(
        df: pd.DataFrame,
        bucket: str,
        prefix: str,
        output_format: str = 'parquet'
    ) -> Dict[str, str]:
        """Upload DataFrame to S3 in specified format(s)."""
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        results = {}
        
        try:
            # Upload Parquet
            if output_format in ['parquet', 'both']:
                try:
                    import pyarrow.parquet as pq
                    import pyarrow as pa
                    
                    parquet_filename = f'feedback_analysis.parquet'
                    parquet_key = f"{prefix.rstrip('/')}/{parquet_filename}"
                    
                    logger.info(f"Uploading Parquet to s3://{bucket}/{parquet_key}")
                    
                    s3_client.put_object(
                        Bucket=bucket,
                        Key=parquet_key,
                        Body=df.to_parquet(engine='pyarrow', compression='snappy', index=False),
                        ContentType='application/octet-stream'
                    )
                    
                    results['parquet'] = f"s3://{bucket}/{parquet_key}"
                    logger.info(f"Parquet uploaded successfully")
                    
                except ImportError:
                    logger.warning("pyarrow not available, skipping Parquet output")
            
            return results
            
        except Exception as e:
            logger.error(f"S3 upload failed: {str(e)}")
            raise


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Main Lambda handler function."""
    logger.info("Lambda execution started - Version 3.1.0 FINAL")
    logger.info(f"Event: {json.dumps(event)}")
    logger.info(f"Output format: {OUTPUT_FORMAT}")
    
    try:
        extractor = DynamoDBExtractor(DYNAMODB_TABLE_NAME)
        
        logger.info("Step 1: Extracting data from DynamoDB")
        raw_items = extractor.scan_table()
        
        if not raw_items:
            logger.warning("No items found in DynamoDB table")
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'No data to process', 'records_processed': 0})
            }
        
        logger.info("Step 2: Transforming data to final dataset")
        final_df = DataTransformer.transform_to_final_dataset(raw_items)
        
        if final_df.empty:
            logger.warning("No feedback records found")
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'No feedback records', 'records_processed': 0})
            }
        
        logger.info("Step 3: Uploading results to S3")
        s3_uris = S3Uploader.upload_dataframe_to_s3(
            df=final_df,
            bucket=S3_BUCKET_NAME,
            prefix=S3_OUTPUT_PREFIX,
            output_format=OUTPUT_FORMAT
        )
        
        response = {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Feedback processing completed successfully',
                'records_processed': len(final_df),
                's3_outputs': s3_uris,
                'output_format': OUTPUT_FORMAT,
                'version': '3.1.0-FINAL',
                'statistics': {
                    'total_feedbacks': len(final_df),
                    'likes': int((final_df['feedback'] == 'like').sum()),
                    'dislikes': int((final_df['feedback'] == 'dislike').sum()),
                    'with_comments': int((final_df['comentario'] != '').sum()),
                    'with_user_messages': int((final_df['mensaje_usuario'] != '').sum()),
                    'with_bot_messages': int((final_df['mensaje_bot'] != '').sum())
                }
            })
        }
        
        logger.info(f"Lambda execution completed successfully")
        return response
        
    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Feedback processing failed', 'error': str(e)})
        }
