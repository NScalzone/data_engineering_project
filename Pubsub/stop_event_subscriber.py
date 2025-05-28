from concurrent.futures import TimeoutError
from google.oauth2 import service_account
from google.cloud import pubsub_v1
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import timeit
import os
import json


subscription_id = "stop_events-sub"
project_id = "data-eng-scalzone"
SERVICE_ACCOUNT_FILE = "data_engineering_project/stop_event_key.json"
CURRENT_DIR = os.path.dirname(__file__)
PARENT_DIR = os.path.dirname(CURRENT_DIR)
SERVICE_ACCOUNT_FILE = os.path.join(PARENT_DIR, "key_pubsub.json") # path to service account key file
# Number of seconds the subscriber should listen for messages
timeout = 60.0

start = timeit.default_timer()

date = datetime.datetime.now()

if not os.path.exists(f'subscriber_stop_events/{date.year}-{date.month}-{date.day}'): os.makedirs(f'subscriber_stop_events/{date.year}-{date.month}-{date.day}')
if not os.path.exists(f'subscriber_stop_events/error/{date.year}-{date.month}'): os.makedirs(f'subscriber_stop_events/error/{date.year}-{date.month}')

COUNT = 0
def increment():
    global COUNT
    COUNT += 1
    if COUNT % 10000 == 0: print(f"Received {COUNT} messages.")
    
# Create a function to log which busses failed to retrieve data
def log_print(error, bus):
    """Prints the error to console and logs the bus to a file."""
    print(error)
    with open(f'subscriber_stop_events/error/{date.year}-{date.month}-{date.day}.txt', 'a') as f: f.write(bus + '\n')

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
        json_obj = json.loads(message.data.decode("utf-8"))

        bus = json_obj[0]["vehicle_number"]
        print(f"retrieved data for bus: {bus}")

        # Save bus data as JSON file
        with open(f'subscriber_stop_events/{date.year}-{date.month}-{date.day}/{bus}.json', 'a') as f: 
            json.dump(json_obj, f, indent=4)

        print(f"Saved stop event for bus: {bus}")
        increment()

    except Exception as e: log_print(e, "message error")

    message.ack()
    
    
while True:
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.

    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.result()  # Block until the shutdown is complete.
            streaming_pull_future.cancel()  # Trigger the shutdown.
            