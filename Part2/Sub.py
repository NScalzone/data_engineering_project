# This program loads data from Google Cloud Pub/Sub into a JSON file, processing vehicle breadcrumbs in real-time.
# File: Sub.py | Name: Kareem T | Date: 05-26-2025
import json, os, datetime
from concurrent import futures
from google.cloud import pubsub_v1
from google.oauth2.service_account import Credentials
import threading
import libs.load as load

# Constants
PARENT_DIR = os.path.dirname(os.path.dirname(__file__))
KEY_FILE = os.path.join(PARENT_DIR, "key_pubsub.json") # path to service account key file
VEHICLE_IDS_FILE = '/srv/DataEng/vehicles.txt' # path to vehicle IDs file
OUTPUT_FILE = f"/srv/DataEng/Data/Breadcrumbs/{datetime.date.today().isoformat()}.json" # Output file path

COUNT, UNDELIVERED, crumbs = 0, 0, [] # Globals

lock = threading.Lock() # Lock for thread safety during file operations
with open(VEHICLE_IDS_FILE, "r") as f: vehicle_ids = [bus.strip() for bus in f.readlines()] # Read vehicle IDs from file.

def callback(message):
    '''Callback function to process incoming Pub/Sub messages.'''
    global crumbs, COUNT, UNDELIVERED
    try:
        crumb = json.loads(message.data.decode("utf-8"))
        with lock: crumbs.append(crumb)
        COUNT += 1
        UNDELIVERED += 1
        if UNDELIVERED % 100000 == 0: 
            print(f"Processed {COUNT} messages")
            with lock:
                with open(OUTPUT_FILE, "w") as f: 
                    json.dump(crumbs, f, indent=2)
                    load.store_pubsub_data(crumbs, 'breadcrumb')
                UNDELIVERED = 0
        message.ack()
    except Exception as e:
        print(f"Error processing message: {e}")
        message.nack()

# (GCP) Google Cloud
project_id = "data-eng-scalzone"
sub_id = "breadcrumbs_sub"
# Create subscriber using credentials
sub = pubsub_v1.SubscriberClient(credentials=(Credentials.from_service_account_file(KEY_FILE)))
sub_path = sub.subscription_path(project_id, sub_id)
streaming_pull_future = sub.subscribe(sub_path, callback=callback)
future_list = [] # This future_list will allow us to create a data structure to inspect in a blocking for loop shortly.

# Infinite main loop to keep the subscriber running
print(f"Listening for messages on {sub_path}...")
while True:
    OUTPUT_FILE = f"/srv/DataEng/Data/Breadcrumbs/{datetime.date.today().isoformat()}.json" # Parse output file
    with lock:
        if not os.path.exists(OUTPUT_FILE): 
            with open(OUTPUT_FILE, "w") as f: json.dump([], f) # Create output file if it doesn't exist.
        with open(OUTPUT_FILE, "r") as f: crumbs = json.load(f) # Load already seen crumbs.
    try: 
        COUNT = 0
        try: streaming_pull_future.result(timeout = 100)
        except futures.TimeoutError:
            if UNDELIVERED > 0:
                with lock:
                    with open(OUTPUT_FILE, "w") as f: 
                        json.dump(crumbs, f, indent=2)
                        load.store_pubsub_data(crumbs, 'breadcrumb')
                print(f"Saved {UNDELIVERED} undelivered messages to {OUTPUT_FILE} and database.")
                UNDELIVERED = 0
        print(f'Processed {COUNT} messages. Output saved to {OUTPUT_FILE}.')
    except KeyboardInterrupt: streaming_pull_future.cancel()
