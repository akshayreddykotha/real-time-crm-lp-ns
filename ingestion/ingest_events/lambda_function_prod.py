# alias: prod
import json
import logging
import boto3
import os
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
BUCKET_NAME = os.environ.get("RAW_EVENTS_BUCKET")

def lambda_handler(event, context):
    """
    Lambda entry point for CRM webhook events
    """

    logger.info(f"Incoming API Gateway event: {json.dumps(event)}")

    try:
        body = json.loads(event.get("body", "{}"))  # CRM sends JSON payload
    except Exception as e:
        logger.error(f"Error parsing body: {e}")
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Invalid JSON"})
        }

    # Ensure body has event structure
    crm_event = body.get("event", {})
    lead_id = crm_event.get("lead_id", "unknown")
    event_id = crm_event.get("id", datetime.utcnow().isoformat())

    # File naming convention: crm_event_{lead_id}_{event_id}.json
    key = f"crm_event_{lead_id}_{event_id}.json"

    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(body),
            ContentType="application/json"
        )
        logger.info(f"Stored CRM event in S3: {key}")

        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"Event stored as {key}"})
        }

    except Exception as e:
        logger.error(f"Error saving to S3: {e}")
        # Return 500 so CRM retries
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Failed to store event"})
        }
