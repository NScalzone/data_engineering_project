# This program loads data from Google Cloud Pub/Sub into a JSON file, processing vehicle breadcrumbs in real-time.
# File: Sub.py | Name: Kareem T | Date: 05-26-2025
import json, os, datetime
from concurrent import futures
from google.cloud import pubsub_v1
from google.oauth2 import service_account
import threading

# Global Variables
# ---Important directories/files
CURRENT_DIR = os.path.dirname(__file__)
PARENT_DIR = os.path.dirname(CURRENT_DIR)
PUBSUB_ACCOUNT_FILE = os.path.join(PARENT_DIR, "key_pubsub.json") # path to service account key file
VEHICLE_IDS_FILE = os.path.join(PARENT_DIR, "vehicles.txt") # path to vehicle IDs file
OUTPUT_FILE = os.path.join(CURRENT_DIR, f"crumbs/breadcrumbs_{datetime.date.today().isoformat()}.json")
print(f'Output file path: {OUTPUT_FILE}')
# ---Data
breadcrumbs = []
message_count = 0
undelivered_messages = 0
lock = threading.Lock() # Lock for thread safety
with open(VEHICLE_IDS_FILE, "r") as f: vehicle_ids = [bus.strip() for bus in f.readlines()] # Read vehicle IDs from file.

# Callback function to process incoming messages
def callback(message):
    global breadcrumbs, message_count, undelivered_messages
    try:
        crumb = json.loads(message.data.decode("utf-8"))
        with lock: breadcrumbs.append(crumb)
        message_count += 1
        undelivered_messages += 1
        if message_count % 100000 == 0: 
            print(f"Processed {message_count} messages")
            with open(OUTPUT_FILE, "w") as f: 
                with lock: json.dump(breadcrumbs, f, indent=4)
                undelivered_messages = 0
        message.ack()
    except Exception as e:
        print(f"Error processing message: {e}")
        message.nack()

# (GCP) Google Cloud
# ---Project and Subscription IDs
project_id = "data-eng-scalzone"
subscription_id = "breadcrumbs_sub"
# ---Create Pub/Sub subscriber using credentials
pubsub_creds =  (service_account.Credentials.from_service_account_file(PUBSUB_ACCOUNT_FILE))
subscriber = pubsub_v1.SubscriberClient(credentials=pubsub_creds)
subscription_path = subscriber.subscription_path(project_id, subscription_id)
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
future_list = [] # This future_list will allow us to create a data structure to inspect in a blocking for loop shortly.

# Ensure output file exists
if not os.path.exists(OUTPUT_FILE): 
    with open(OUTPUT_FILE, "w") as f: json.dump([], f)
with open(OUTPUT_FILE, "r") as f: # Load existing content
    try: breadcrumbs = json.load(f)
    except json.JSONDecodeError: breadcrumbs = []

# Infinite main loop to keep the subscriber running
print(f"Listening for messages on {subscription_path}...")
while True:
    try: 
        message_count = 0
        try:
            streaming_pull_future.result(timeout = 100)
        except futures.TimeoutError:
            if undelivered_messages > 0:
                print(f"Processed {message_count} messages with {undelivered_messages} undelivered.")
                with open(OUTPUT_FILE, "w") as f: 
                    with lock: json.dump(breadcrumbs, f, indent=4)
                undelivered_messages = 0
            elif message_count > 0:
                print(f"Processed {message_count} messages.")
            else: print("No messages processed.")
    except KeyboardInterrupt: streaming_pull_future.cancel()

