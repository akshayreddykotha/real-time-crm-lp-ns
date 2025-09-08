# alias: prod
import json
import logging
import boto3
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
BUCKET_NAME = os.environ.get("RAW_EVENTS_BUCKET")

def lambda_handler(event, context):
    """
    Lambda entry point for CRM webhook events
    """

    # 1. Log the raw event (API Gateway passes request body inside "body")
    logger.info(f"Incoming event: {json.dumps(event)}")

    try:
        body = json.loads(event["body"])  # CRM sends JSON payload
    except Exception as e:
        logger.error(f"Error parsing body: {e}")
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Invalid JSON"})
        }

    # 2. Extract lead_id safely
    lead_id = body.get("lead_id", "unknown")

    # 3. File name convention
    key = f"crm_event_{lead_id}.json"

    # 4. Save to S3
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(body),
        ContentType="application/json"
    )

    logger.info(f"Stored event in S3: {key}")

    return {
        "statusCode": 200,
        "body": json.dumps({"message": f"Event stored as {key}"})
    }
