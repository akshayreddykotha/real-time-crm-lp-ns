# lookup lead data from public bbucket, gather webhooks and lead owner data, send a notification to slack (fall back to SES after 3 retries)

import json
import boto3
import time
import urllib.request

s3 = boto3.client('s3')
secretsmanager = boto3.client('secretsmanager')
ses = boto3.client("ses")

LOOKUP_BASE_URL = "https://dea-lead-owner.s3.us-east-1.amazonaws.com"
SLACK_SECRET_NAME = "SlacckWebhookURL"

# Cache webhook URL to avoid fetching on every invocation
slack_webhook_url = None

def get_slack_webhook_url():
    global slack_webhook_url
    if slack_webhook_url is None:
        try:
            response = secretsmanager.get_secret_value(SecretId=SLACK_SECRET_NAME)
            secret = json.loads(response['SecretString'])
            slack_webhook_url = secret.get("slackWebhookURL") # key-name
        except Exception as e:
            print(f"Error fetching Slack secret: {e}")
            return None
    return slack_webhook_url

def send_slack_notification(lead):
    url = get_slack_webhook_url()
    if not url:
        print("No Slack webhook URL available.")
        return
    
    message = {
        "text": (
            f"*New Lead Enriched:*\n"
            f"• Name: {lead.get('Name')}\n"
            f"• Lead ID: {lead.get('Lead ID')}\n"
            f"• Created Date: {lead.get('Created Date')}\n"
            f"• Label: {lead.get('Label')}\n"
            f"• Email: {lead.get('Email')}\n"
            f"• Lead Owner: {lead.get('Lead Owner')}\n"
            f"• Funnel: {lead.get('Funnel')}"
        )
    }

    data = json.dumps(message).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})

     # Retry up to 3 times before falling back
    for attempt in range(1, 4):
        try:
            with urllib.request.urlopen(req) as response:
                print(f"Slack notification sent on attempt {attempt}:", response.read().decode())
                return
        except Exception as e:
            print(f"Slack notification attempt {attempt} failed: {e}")
            if attempt < 3:
                # Exponential backoff (1s, 2s)
                sleep_time = 2 ** (attempt - 1)
                print(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                print("All Slack attempts failed. Falling back to SES.")
                send_email_fallback(lead)

def send_email_fallback(lead):
    try:
        response = ses.send_email(
            Source="iitk1948@gmail.com",
            Destination={
                "ToAddresses": ["iitk1948@gmail.com"]
            },
            Message={
                "Subject": {"Data": "CRM Lead Notification (Slack Fallback)"},
                "Body": {
                    "Text": {
                        "Data": (
                            f"Lead Details:\n"
                            f"Name: {lead.get('Name')}\n"
                            f"Lead ID: {lead.get('Lead ID')}\n"
                            f"Email: {lead.get('Email')}\n"
                            f"Owner: {lead.get('Lead Owner')}\n"
                            f"Funnel: {lead.get('Funnel')}\n"
                        )
                    }
                }
            }
        )
        print("SES fallback email sent:", response)
    except Exception as e:
        print(f"SES fallback failed: {e}")

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

            # 🔹 Send Slack notification + fall back SES
            send_slack_notification(merged)