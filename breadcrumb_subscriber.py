from concurrent.futures import TimeoutError
from google.oauth2 import service_account
from google.cloud import pubsub_v1
from bs4 import BeautifulSoup
import datetime
import timeit
import os
import json

subscription_id = "breadcrumbs_sub"
project_id = "data-eng-scalzone"
SERVICE_ACCOUNT_FILE = "data_engineering_project/stop_event_key.json"


# Number of seconds the subscriber should listen for messages
# timeout = 60.0

start = timeit.default_timer()

date = datetime.datetime.now()

if not os.path.exists(f'subscriber_breadcrumbs/{date.year}/{date.month}/{date.day}'): os.makedirs(f'subscriber_breadcrumbs/{date.year}/{date.month}/{date.day}')
if not os.path.exists(f'subscriber_breadcrumbs/error/{date.year}/{date.month}'): os.makedirs(f'subscriber_breadcrumbs/error/{date.year}/{date.month}')

COUNT = 0
def increment():
    global COUNT
    COUNT += 1
    if COUNT % 10000 == 0: print(f"Received {COUNT} messages.")
    
# Create a function to log which busses failed to retrieve data
def log_print(error, bus):
    """Prints the error to console and logs the bus to a file."""
    print(error)
    with open(f'subscriber_breadcrumbs/error/{date.year}/{date.month}/{date.day}.txt', 'a') as f: f.write(bus + '\n')

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
    try:
        # Parse the JSON
        json_obj = json.loads(message.data.decode("utf-8"))
        bus = json_obj["VEHICLE_ID"]

        # Save bus data as JSON file
        with open(f'subscriber_breadcrumbs/{date.year}/{date.month}/{date.day}/{bus}.json', 'a') as f: json.dump(json_obj, f, indent=4)
       
        increment()

    except Exception as e: log_print(e, "message error")

    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

# Wrap subscriber in a 'with' block to automatically call close() when done.

with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result()
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.
        
print(f"Received {COUNT} messages.")
stop = timeit.default_timer()
print('Time: ', stop - start)  