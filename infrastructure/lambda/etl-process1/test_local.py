# test_local.py
import os
os.environ['DYNAMODB_TABLE_NAME'] = 'cat-prod-catia-conversations-table'
os.environ['S3_BUCKET_NAME'] = 'cat-test-normalize-reports'

from feedback_analysis import lambda_handler
response = lambda_handler({}, None)
