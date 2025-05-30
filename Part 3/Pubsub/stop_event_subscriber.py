from concurrent.futures import TimeoutError
from google.oauth2 import service_account
from google.cloud import pubsub_v1
import datetime
import timeit
import os
import json
import threading
import libs.load as load

subscription_id = "stop_events-sub"
project_id = "data-eng-scalzone"
CURRENT_DIR = os.path.dirname(__file__)
PARENT_DIR = os.path.dirname(CURRENT_DIR)
SERVICE_ACCOUNT_FILE = os.path.join(PARENT_DIR, "key_pubsub.json") # path to service account key file
# Number of seconds the subscriber should listen for messages
timeout = 60.0
start = timeit.default_timer()
date = datetime.date.today().isoformat()

stop_events = []
lock = threading.Lock()

COUNT = 0
UNDELIVERED = 0
OUTPUT_FILE = os.path.join(CURRENT_DIR, f"subscriber_stop_events/stop_{date}.json")

def increment():
    global COUNT, UNDELIVERED
    COUNT += 1
    UNDELIVERED += 1
    if COUNT % 10000 == 0: print(f"Received {COUNT} messages.")
    

pubsub_creds =  (
    service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE
        )
    )

subscriber = pubsub_v1.SubscriberClient(credentials=pubsub_creds)
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    print("arrived at callback")
    try:
        global UNDELIVERED, stop_events
        json_obj = json.loads(json.loads(message.data.decode("utf-8")))

        if isinstance(json_obj, dict): 
            with lock: 
                stop_events.append(json_obj)
            bus = json_obj["vehicle_number"]
        elif isinstance(json_obj, list): 
            with lock: 
                stop_events.extend(json_obj)
            bus = json_obj[0]["vehicle_number"]
        else: print(f'The type of object received ({type(json_obj)}) is unknown')

        print(f"retrieved data for bus: {bus}")
        if not os.path.exists(OUTPUT_FILE):
            with open(OUTPUT_FILE, 'w') as f: json.dump([], f)

        # Save bus data as JSON file
        if COUNT % 1000 == 0:
            with lock:
                with open(OUTPUT_FILE, 'a') as f: 
                    json.dump(stop_events, f, indent=2)
                    load.store_pubsub_data(stop_events, 'trip')
                    UNDELIVERED = 0
                    stop_events = []

        print(f"Saved stop event for bus: {bus}")
        increment()

    except Exception as e: print(e)

    message.ack()
    
    
while True:
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}.. to output at {OUTPUT_FILE}\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.

    with subscriber:
        OUTPUT_FILE = os.path.join(CURRENT_DIR, f"subscriber_stop_events/stop_{datetime.date.today().isoformat()}.json")
        COUNT = 0
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result(timeout=timeout)
            print(f"Processed {COUNT} messages.")
        except TimeoutError:
            if UNDELIVERED > 0:
                print(f"Processed {COUNT} messages with {UNDELIVERED} undelivered.")
                with open(OUTPUT_FILE, "w") as f: 
                    with lock: json.dump(stop_events, f, indent=2)
                UNDELIVERED = 0
            elif COUNT > 0: print(f"Processed {COUNT} messages.")
            else: 
                print("No messages processed.")
                stop_events = []
            streaming_pull_future.result()  # Block until the shutdown is complete.
            streaming_pull_future.cancel()  # Trigger the shutdown.
            