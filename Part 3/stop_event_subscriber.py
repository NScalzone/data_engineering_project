from concurrent.futures import TimeoutError
from google.oauth2 import service_account
from google.cloud import pubsub_v1
import datetime
import timeit
import os
import json
import threading
import libs.load as load

PARENT_DIR = os.path.dirname(os.path.dirname(__file__))
KEY_FILE = os.path.join(PARENT_DIR, "key_pubsub.json") # path to service account key file

timeout = 60.0 # Number of seconds the subscriber should listen for messages
start = timeit.default_timer()
date = datetime.date.today().isoformat()

stop_events = []
lock = threading.Lock()

COUNT = 0
UNDELIVERED = 0  # Counter for undelivered messages
OUTPUT_FILE = f"/srv/DataEng/Data/Stops/{date}.json"

sub_id = "stop_events-sub"
project_id = "data-eng-scalzone"
pubsub_creds =  (service_account.Credentials.from_service_account_file(KEY_FILE))
sub = pubsub_v1.SubscriberClient(credentials=pubsub_creds)
sub_path = sub.subscription_path(project_id, sub_id) # fully qualified identifier in the form `projects/{project_id}/subscriptions/{subscription_id}`

def increment():
    '''Increment the global message count and undelivered count.'''
    global COUNT, UNDELIVERED
    COUNT += 1
    UNDELIVERED += 1
    if COUNT % 10000 == 0: print(f"Received {COUNT} messages.")
    
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    '''Callback function to process incoming Pub/Sub messages.'''
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
        if UNDELIVERED % 500 == 0:
            with lock:
                with open(OUTPUT_FILE, 'w') as f: 
                    json.dump(stop_events, f, indent=2)
                    load.store_pubsub_data(stop_events, 'trip')
                    UNDELIVERED = 0
        increment()
    except Exception as e: print(e)
    message.ack()
    
while True:
    streaming_pull_future = sub.subscribe(sub_path, callback=callback)
    print(f"Listening for messages on {sub_path}.. to output at {OUTPUT_FILE}\n")
    with sub:
        OUTPUT_FILE = f"/srv/DataEng/Data/Stops/{datetime.date.today().isoformat()}.json"
        COUNT = 0
        stop_events = json.load(open(OUTPUT_FILE, "r")) if os.path.exists(OUTPUT_FILE) else []
        try: streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            if UNDELIVERED > 0:
                print(f"{UNDELIVERED} messages undelivered.")
                with lock:
                    with open(OUTPUT_FILE, "w") as f: json.dump(stop_events, f, indent=2)
                UNDELIVERED = 0
            streaming_pull_future.result()  # Block until the shutdown is complete.
            streaming_pull_future.cancel()  # Trigger the shutdown.
        print(f"Processed {COUNT} messages.")