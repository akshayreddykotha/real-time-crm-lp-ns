# process CRM events off of SQS message
import json
import boto3

s3 = boto3.client('s3')

def lambda_handler(event, context):
    for record in event['Records']:
        # SQS message body contains the S3 event notification
        message = json.loads(record['body'])
        s3_event = json.loads(message['Message']) if "Message" in message else message

        for s3_record in s3_event.get('Records', []):
            bucket = s3_record['s3']['bucket']['name']
            key = s3_record['s3']['object']['key']

            print(f"Fetching file from {bucket}/{key}")
            response = s3.get_object(Bucket=bucket, Key=key)
            file_content = response['Body'].read().decode('utf-8')
            
            # Parse JSON leads
            lead_data = json.loads(file_content)
            print("Lead Data:", lead_data)
            
            # TODO: Send to downstream service / transform / store in DB
