# test mock lead parallel upload into the s3 bucket
import boto3
import json
import uuid
from datetime import datetime

s3 = boto3.client("s3")

# 🔹 Replace with your CRM bucket name
BUCKET_NAME = "ak-crm-webhooks"
RAW_PREFIX = "crm_event_lead_"

def lambda_handler(event, context):
    # How many leads to generate? (default = 10)
    count = event.get("count", 10)

    # Shared timestamp so they look simultaneous
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    uploaded_keys = []

    for i in range(count):
        lead_id = str(uuid.uuid4())
        mock_event = {
            "event": {
                "lead_id": lead_id,
                "data": {
                    "display_name": f"Test Lead {i+1}",
                    "date_created": timestamp,
                    "status_label": "New"
                }
            }
        }

        key = f"{RAW_PREFIX}{lead_id}_test.json"
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(mock_event),
            ContentType="application/json"
        )

        uploaded_keys.append(key)

    return {
        "message": f"Uploaded {count} mock leads",
        "files": uploaded_keys
    }
