# lookup lead data from public bbucket and gather webhooks and lead owner data

import json
import boto3
import urllib.request

s3 = boto3.client('s3')

LOOKUP_BASE_URL = "https://dea-lead-owner.s3.us-east-1.amazonaws.com"

def fetch_lookup(lead_id):
    url = f"{LOOKUP_BASE_URL}/{lead_id}.json"
    try:
        with urllib.request.urlopen(url) as response:
            return json.loads(response.read().decode('utf-8'))
    except Exception as e:
        print(f"Lookup failed for {lead_id}: {e}")
        return {}

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
            ld = json.loads(file_content)
            # print("Lead Data:", lead_data)

            crm_event = ld.get("event", "{}")
            lead_id = crm_event.get("lead_id", "unknown")
            lead_data = crm_event.get("data", {})
            
             # Extract CRM event fields
            crm_details = {
                "Name": lead_data.get("display_name"),
                "Lead ID": lead_id,
                "Created Date": lead_data.get("date_created"),
                "Label": lead_data.get("status_label"),
            }

            # Fetch lookup JSON
            lookup_data = fetch_lookup(lead_id)

            # Join both sources
            merged = {
                **crm_details,
                "Email": lookup_data.get("lead_email"),
                "Lead Owner": lookup_data.get("lead_owner"),
                "Funnel": lookup_data.get("funnel"),
            }

            print("Merged Lead Data:", merged)


            # TODO: send merged downstream (DB, API, or another S3 bucket)

            # Save enriched file into enriched/ folder
            enriched_key = f"enriched/{lead_id}.json"
            s3.put_object(
                Bucket=bucket,
                Key=enriched_key,
                Body=json.dumps(merged),
                ContentType="application/json"
            )
            print(f"Enriched data saved at: s3://{bucket}/{enriched_key}")