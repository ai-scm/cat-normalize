"""
Lambda Function: Feedback Processor for DynamoDB Chatbot Data

Description:
    Processes DynamoDB records containing user conversations and feedback,
    associating each feedback with its corresponding conversation message
    based on timestamp proximity heuristics.
    
    Includes both user message and bot message in output.

Author: Data Engineering Team
Version: 2.0.0
"""

import json
import os
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from decimal import Decimal

import boto3
import pandas as pd
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables with defaults
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME', 'cat-prod-catia-conversations-table')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'cat-prod-feedback-reports')
S3_OUTPUT_PREFIX = os.environ.get('S3_OUTPUT_PREFIX', 'feedback-analysis/')

# AWS clients
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
s3_client = boto3.client('s3', region_name='us-east-1')

# Constants
FINAL_COLUMNS = [
    'usuario_id',
    'nombre',
    'fecha',
    'mensaje_usuario',
    'mensaje_bot',
    'feedback',
    'comentario',
    'opcion_respuesta',
    'ciudad',
    'gerencia',
    'oficinas_asesoras',
    'subgerencia',
    'nivel_directivo'
]


class DynamoDBExtractor:
    """Handles extraction of data from DynamoDB table."""
    
    def __init__(self, table_name: str):
        """
        Initialize DynamoDB extractor.
        
        Args:
            table_name: Name of the DynamoDB table to query
        """
        self.table_name = table_name
        self.table = dynamodb.Table(table_name)
        logger.info(f"Initialized DynamoDBExtractor for table: {table_name}")
    
    def scan_table(self) -> List[Dict[str, Any]]:
        """
        Scan entire DynamoDB table with pagination support.
        
        Returns:
            List of all items from the table
            
        Raises:
            ClientError: If DynamoDB scan operation fails
        """
        logger.info("Starting DynamoDB table scan")
        items = []
        
        try:
            response = self.table.scan()
            items.extend(response.get('Items', []))
            
            # Handle pagination
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
    """Parses and transforms raw DynamoDB data into structured format."""
    
    @staticmethod
    def parse_dynamodb_value(value: Any) -> Any:
        """
        Recursively parse DynamoDB formatted value to Python native types.
        
        Args:
            value: DynamoDB formatted value (e.g., {"S": "text"}, {"N": "123"})
            
        Returns:
            Parsed Python native value
        """
        if not isinstance(value, dict):
            return value
        
        # String type
        if 'S' in value:
            return value['S']
        
        # Number type
        if 'N' in value:
            try:
                return int(value['N']) if '.' not in value['N'] else float(value['N'])
            except ValueError:
                return value['N']
        
        # Boolean type
        if 'BOOL' in value:
            return value['BOOL']
        
        # Null type
        if 'NULL' in value:
            return None
        
        # List type
        if 'L' in value:
            return [DataParser.parse_dynamodb_value(item) for item in value['L']]
        
        # Map type
        if 'M' in value:
            return {k: DataParser.parse_dynamodb_value(v) for k, v in value['M'].items()}
        
        return value
    
    @staticmethod
    def extract_user_data(user_data: Any) -> Dict[str, str]:
        """
        Extract and parse UserData field.
        
        Args:
            user_data: Raw UserData from DynamoDB item
            
        Returns:
            Dictionary with parsed user information
        """
        default_user_data = {
            'nombre': '',
            'ciudad': '',
            'gerencia': '',
            'oficinas_asesoras': '',
            'subgerencia': '',
            'nivel_directivo': ''
        }
        
        if not user_data:
            return default_user_data
        
        parsed_data = DataParser.parse_dynamodb_value(user_data)
        
        if not isinstance(parsed_data, dict):
            return default_user_data
        
        return {
            'nombre': parsed_data.get('nombre', ''),
            'ciudad': parsed_data.get('ciudad', ''),
            'gerencia': parsed_data.get('gerencia', ''),
            'oficinas_asesoras': parsed_data.get('oficinas_asesoras', ''),
            'subgerencia': parsed_data.get('subgerencia', ''),
            'nivel_directivo': parsed_data.get('nivel_directivo', '')
        }
    
    @staticmethod
    def extract_conversation_messages(conversation: Any) -> List[Dict[str, str]]:
        """
        Extract and parse Conversation field to get all messages.
        
        Args:
            conversation: Raw Conversation array from DynamoDB
            
        Returns:
            List of message dictionaries with 'from', 'text', 'timestamp'
        """
        if not conversation:
            return []
        
        parsed_conversation = DataParser.parse_dynamodb_value(conversation)
        
        if not isinstance(parsed_conversation, list):
            return []
        
        messages = []
        for message in parsed_conversation:
            if isinstance(message, dict):
                messages.append({
                    'from': message.get('from', ''),
                    'text': message.get('text', ''),
                    'timestamp': message.get('timestamp', '')
                })
        
        return messages
    
    @staticmethod
    def extract_feedback(feedback: Any) -> Dict[str, str]:
        """
        Extract and parse Feedback field.
        
        Args:
            feedback: Raw Feedback from DynamoDB item
            
        Returns:
            Dictionary with feedback type, comment, and option
        """
        default_feedback = {
            'type': '',
            'comment': '',
            'option': ''
        }
        
        if not feedback:
            return default_feedback
        
        parsed_feedback = DataParser.parse_dynamodb_value(feedback)
        
        if not isinstance(parsed_feedback, dict):
            return default_feedback
        
        return {
            'type': parsed_feedback.get('type', ''),
            'comment': parsed_feedback.get('comment', ''),
            'option': parsed_feedback.get('option', '')
        }


class ConversationExtractor:
    """Extracts user and bot messages from conversation data."""
    
    @staticmethod
    def get_user_and_bot_messages(messages: List[Dict[str, str]]) -> Tuple[str, str]:
        """
        Extract user message and bot response from conversation messages.
        
        Logic:
            - Finds the last user message in the conversation
            - Finds the first bot message that comes after the user message
            - If no clear pairing exists, takes last user message and last bot message
        
        Args:
            messages: List of message dictionaries with 'from' and 'text'
            
        Returns:
            Tuple of (user_message, bot_message)
        """
        if not messages:
            return ('', '')
        
        user_messages = []
        bot_messages = []
        
        # Separate messages by type
        for i, msg in enumerate(messages):
            msg_from = msg.get('from', '').lower()
            msg_text = msg.get('text', '')
            
            if msg_from == 'user':
                user_messages.append({'index': i, 'text': msg_text})
            elif msg_from in ['bot', 'assistant', 'system']:
                bot_messages.append({'index': i, 'text': msg_text})
        
        # If no messages found
        if not user_messages and not bot_messages:
            return ('', '')
        
        if not user_messages:
            # Only bot messages exist
            return ('', bot_messages[-1]['text'])
        
        if not bot_messages:
            # Only user messages exist
            return (user_messages[-1]['text'], '')
        
        # Try to find the last user message and its corresponding bot response
        last_user = user_messages[-1]
        
        # Find first bot message after the last user message
        bot_response = None
        for bot_msg in bot_messages:
            if bot_msg['index'] > last_user['index']:
                bot_response = bot_msg['text']
                break
        
        # If no bot response after user message, take the last bot message
        if bot_response is None:
            bot_response = bot_messages[-1]['text']
        
        return (last_user['text'], bot_response)


class FeedbackProcessor:
    """Associates feedback with corresponding conversation messages."""
    
    @staticmethod
    def parse_timestamp(timestamp_str: str) -> Optional[datetime]:
        """
        Parse timestamp string to datetime object.
        
        Args:
            timestamp_str: Timestamp in ISO format
            
        Returns:
            Datetime object or None if parsing fails
        """
        if not timestamp_str:
            return None
        
        try:
            # Handle ISO format with microseconds
            return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except (ValueError, AttributeError) as e:
            logger.warning(f"Failed to parse timestamp: {timestamp_str}, error: {str(e)}")
            return None
    
    @staticmethod
    def extract_sk_type_and_timestamp(sk: str) -> Tuple[str, Optional[datetime]]:
        """
        Extract type and timestamp from Sort Key.
        
        Args:
            sk: Sort Key value (e.g., "CONVERSATION#2025-08-18T13:19:49.966664")
            
        Returns:
            Tuple of (type, timestamp) where type is 'CONVERSATION', 'FEEDBACK', or 'REGISTER'
        """
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
        """
        Associate each feedback with its corresponding conversation based on timestamp.
        
        Logic:
            For each feedback, find the conversation with the most recent timestamp
            that is still before the feedback timestamp (for the same user).
        
        Args:
            conversations: List of conversation records
            feedbacks: List of feedback records
            
        Returns:
            List of feedback records with associated conversation data
        """
        logger.info(f"Associating {len(feedbacks)} feedbacks with {len(conversations)} conversations")
        
        # Group conversations by user_id
        conversations_by_user: Dict[str, List[Dict[str, Any]]] = {}
        for conv in conversations:
            user_id = conv.get('usuario_id')
            if user_id:
                if user_id not in conversations_by_user:
                    conversations_by_user[user_id] = []
                conversations_by_user[user_id].append(conv)
        
        # Sort each user's conversations by timestamp
        for user_id in conversations_by_user:
            conversations_by_user[user_id].sort(
                key=lambda x: x.get('timestamp') or datetime.min
            )
        
        associated_feedbacks = []
        
        for feedback in feedbacks:
            user_id = feedback.get('usuario_id')
            feedback_timestamp = feedback.get('timestamp')
            
            if not user_id or not feedback_timestamp:
                logger.warning(f"Feedback missing user_id or timestamp: {feedback}")
                continue
            
            # Get conversations for this user
            user_conversations = conversations_by_user.get(user_id, [])
            
            # Find the most recent conversation before this feedback
            matching_conversation = None
            for conv in reversed(user_conversations):
                conv_timestamp = conv.get('timestamp')
                if conv_timestamp and conv_timestamp < feedback_timestamp :
                    matching_conversation = conv
                    break
            
            if matching_conversation:
                # Merge conversation data into feedback
                feedback['mensaje_usuario'] = matching_conversation.get('mensaje_usuario', '')
                feedback['mensaje_bot'] = matching_conversation.get('mensaje_bot', '')
                feedback['fecha_conversacion'] = matching_conversation.get('fecha', '')
                
                logger.debug(
                    f"Associated feedback at {feedback_timestamp} "
                    f"with conversation at {matching_conversation.get('timestamp')}"
                )
            else:
                logger.warning(
                    f"No matching conversation found for feedback. "
                    f"User: {user_id}, Feedback timestamp: {feedback_timestamp}"
                )
                feedback['mensaje_usuario'] = ''
                feedback['mensaje_bot'] = ''
                feedback['fecha_conversacion'] = ''
            
            associated_feedbacks.append(feedback)
        
        logger.info(f"Successfully associated {len(associated_feedbacks)} feedbacks")
        return associated_feedbacks


class DataTransformer:
    """Transforms raw data into final dataset structure."""
    
    @staticmethod
    def transform_to_final_dataset(items: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Transform DynamoDB items into final dataset structure.
        
        Args:
            items: List of raw DynamoDB items
            
        Returns:
            DataFrame with final structure including user and bot messages
        """
        logger.info("Starting data transformation to final dataset")
        
        conversations = []
        feedbacks = []
        
        # Separate conversations and feedbacks
        for item in items:
            pk = item.get('PK', '')
            sk = item.get('SK', '')
            
            # Extract user_id from PK
            usuario_id = pk.replace('USER#', '') if pk.startswith('USER#') else pk
            
            # Get SK type and timestamp
            sk_type, timestamp = FeedbackProcessor.extract_sk_type_and_timestamp(sk)
            
            # Skip REGISTER records
            if sk_type == 'REGISTER':
                continue
            
            # Extract common fields
            user_data = DataParser.extract_user_data(item.get('UserData'))
            created_at = item.get('CreatedAt', '')
            
            # Parse CreatedAt if available
            fecha = FeedbackProcessor.parse_timestamp(created_at)
            fecha_str = fecha.strftime('%Y-%m-%d') if fecha else ''
            
            # Process CONVERSATION records
            if sk_type == 'CONVERSATION':
                conversation_messages = DataParser.extract_conversation_messages(
                    item.get('Conversation')
                )
                
                # Extract user message and bot message
                user_message, bot_message = ConversationExtractor.get_user_and_bot_messages(
                    conversation_messages
                )
                
                conversations.append({
                    'usuario_id': usuario_id,
                    'timestamp': timestamp,
                    'fecha': fecha_str,
                    'mensaje_usuario': user_message,
                    'mensaje_bot': bot_message,
                    **user_data
                })
            
            # Process FEEDBACK records
            elif sk_type == 'FEEDBACK':
                feedback_data = DataParser.extract_feedback(item.get('Feedback'))
                
                feedbacks.append({
                    'usuario_id': usuario_id,
                    'timestamp': timestamp,
                    'fecha': fecha_str,
                    'feedback': feedback_data.get('type', ''),
                    'comentario': feedback_data.get('comment', ''),
                    'opcion_respuesta': feedback_data.get('option', ''),
                    **user_data
                })
        
        logger.info(f"Extracted {len(conversations)} conversations and {len(feedbacks)} feedbacks")
        
        # Associate feedbacks with conversations
        associated_feedbacks = FeedbackProcessor.associate_feedback_to_conversations(
            conversations,
            feedbacks
        )
        
        # Convert to DataFrame
        df = pd.DataFrame(associated_feedbacks)
        
        # Ensure all required columns exist
        for col in FINAL_COLUMNS:
            if col not in df.columns:
                df[col] = ''
        
        # Select and order columns
        df = df[FINAL_COLUMNS]
        
        # Clean data
        df = df.fillna('')
        
        logger.info(f"Final dataset created with {len(df)} records")
        return df


class S3Uploader:
    """Handles uploading processed data to S3."""
    
    @staticmethod
    def upload_dataframe_to_s3(
        df: pd.DataFrame,
        bucket: str,
        prefix: str,
        filename: Optional[str] = None
    ) -> str:
        """
        Upload DataFrame as CSV to S3.
        
        Args:
            df: DataFrame to upload
            bucket: S3 bucket name
            prefix: S3 key prefix
            filename: Optional custom filename (auto-generated if not provided)
            
        Returns:
            S3 URI of uploaded file
            
        Raises:
            ClientError: If S3 upload fails
        """
        if filename is None:
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            filename = f'feedback_analysis_{timestamp}.csv'
        
        s3_key = f"{prefix.rstrip('/')}/{filename}"
        
        logger.info(f"Uploading DataFrame to S3: s3://{bucket}/{s3_key}")
        
        try:
            # Convert DataFrame to CSV
            csv_buffer = df.to_csv(index=False, encoding='utf-8')
            
            # Upload to S3
            s3_client.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=csv_buffer.encode('utf-8'),
                ContentType='text/csv',
                ContentEncoding='utf-8'
            )
            
            s3_uri = f"s3://{bucket}/{s3_key}"
            logger.info(f"Successfully uploaded file to {s3_uri}")
            return s3_uri
            
        except ClientError as e:
            logger.error(f"S3 upload failed: {str(e)}")
            raise


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler function.
    
    Process:
        1. Extract data from DynamoDB
        2. Parse and transform data
        3. Associate feedbacks with conversations (including user and bot messages)
        4. Generate final dataset
        5. Upload to S3
    
    Args:
        event: Lambda event object
        context: Lambda context object
        
    Returns:
        Response dictionary with status and results
    """
    logger.info("Lambda execution started")
    logger.info(f"Event: {json.dumps(event)}")
    
    try:
        # Initialize components
        extractor = DynamoDBExtractor(DYNAMODB_TABLE_NAME)
        
        # Step 1: Extract data from DynamoDB
        logger.info("Step 1: Extracting data from DynamoDB")
        raw_items = extractor.scan_table()
        
        if not raw_items:
            logger.warning("No items found in DynamoDB table")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No data to process',
                    'records_processed': 0
                })
            }
        
        # Step 2: Transform data
        logger.info("Step 2: Transforming data to final dataset")
        final_df = DataTransformer.transform_to_final_dataset(raw_items)
        
        if final_df.empty:
            logger.warning("No feedback records found after transformation")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No feedback records to process',
                    'records_processed': 0
                })
            }
        
        # Step 3: Upload to S3
        logger.info("Step 3: Uploading results to S3")
        s3_uri = S3Uploader.upload_dataframe_to_s3(
            df=final_df,
            bucket=S3_BUCKET_NAME,
            prefix=S3_OUTPUT_PREFIX
        )
        
        # Prepare response
        response = {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Feedback processing completed successfully',
                'records_processed': len(final_df),
                's3_output': s3_uri,
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
        
        logger.info(f"Lambda execution completed successfully: {response}")
        return response
        
    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Feedback processing failed',
                'error': str(e)
            })
        }