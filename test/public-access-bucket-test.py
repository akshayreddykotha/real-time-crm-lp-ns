# this will give a 403 on lead_id that are a day old from adding to ak-crm-webhooks bucket
import requests

# https://us-east-1.console.aws.amazon.com/s3/buckets/dea-lead-owner?region=us-east-1&bucketType=general
lead_id = "lead_niuYPXlw6vnFQIhwZCKDaaKv9XMQs9KA5NgxhNRBgaA"
bucket_name = "dea-lead-owner"
file_name = f"{lead_id}.json"
public_url = f"https://{bucket_name}.s3.us-east-1.amazonaws.com/{file_name}"

# public_url = "https://s3.us-east-1.amazonaws.com/dea-lead-owner/{lead_id}.json"

print(public_url)
    
r = requests.get(public_url)

print(r.status_code)  # should be 200 if accessible
if r.status_code == 200:
    print(r.json())   # prints the file contents
