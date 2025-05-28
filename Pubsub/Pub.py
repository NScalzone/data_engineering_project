# This program loads data from the transit system's API and publishes vehicle breadcrumb data to Google Cloud Pub/Sub.
# File: Pub.py | Name: Kareem T | Date: 05-26-2025
import json, os, datetime, requests
from concurrent import futures
from google.cloud import pubsub_v1
from google.oauth2 import service_account

def future_callback(future):
    try: future.result() # Wait for result of publish operation.
    except Exception as e: print(f"An error occurred: {e}")

# Global Variables
# ---Important directories/files
CURRENT_DIR = os.path.dirname(__file__)
PARENT_DIR = os.path.dirname(CURRENT_DIR)
SERVICE_ACCOUNT_FILE = os.path.join(PARENT_DIR, "key_pubsub.json") # path to service account key file
VEHICLE_IDS_FILE = os.path.join(PARENT_DIR, "vehicles.txt") # path to vehicle IDs file
# ---Data
with open(VEHICLE_IDS_FILE, "r") as f: vehicle_ids = [bus.strip() for bus in f.readlines()] # Read vehicle IDs from file.
count = 0 # Track the records.
startTime = datetime.datetime.now() 
future_list = [] # This future_list will allow us to create a data structure to inspect in a blocking for loop shortly.

# (GCP) Google Cloud
# ---Project and Subscription IDs
project_id = "data-eng-scalzone"
topic_id = "my-topic"
# ---Create Pub/Sub subscriber using credentials
pubsub_creds =  (service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE))
publisher = pubsub_v1.PublisherClient(credentials=pubsub_creds)
topic_path = publisher.topic_path(project_id, topic_id)

# Loop through and fetch data
for vehicle_id in vehicle_ids:
    if int(vehicle_id) < 4032: continue
    url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"
    response = requests.get(url)
    if response.status_code == 200:
        try:
            data = response.json()
            # Loop through each breadcrumb and publish to Pub/Sub
            for breadcrumb in data:
                message_data = json.dumps(breadcrumb).encode("utf-8")
                future = publisher.publish(topic_path, data=message_data)
                future.add_done_callback(future_callback)     # This is the callback we use to essentially acknowledge the result.
                future_list.append(future)    # Append this new future to our list of futures.
                count += 1
                if count % 50000 == 0:
                    print(f"Published breadcrumb for vehicle {vehicle_id} (current: {count})")
        except Exception as e: print(f"Failed to process JSON for vehicle {vehicle_id}: {e}")
    else: print(f"Failed to fetch for {vehicle_id} (Status: {response.status_code})")
print(f"Published {count} messages to {topic_path} in {datetime.datetime.now() - startTime} time.")
for future in futures.as_completed(future_list): 
    continue
