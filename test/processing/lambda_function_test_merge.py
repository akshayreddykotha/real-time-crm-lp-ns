# test lookup lead data from public bucket and gather webhooks and lead owner data
# input would be a lambda test event given in the AWS Lambda function console

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
                  
    # Parse JSON leads
    # print("Lead Data:", lead_data)

    crm_event = event.get("event", "{}")
    lead_id = crm_event.get("lead_id", "unknown")
    lead_data = crm_event.get("data", {})

    # Extract CRM event lead details
    
    crm_details = {
        "Name": lead_data.get("display_name"),
        "Lead ID": lead_id,
        "Created Date": crm_event.get("date_created"),
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