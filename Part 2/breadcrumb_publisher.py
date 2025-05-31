import json
import os
from urllib.request import urlopen
from concurrent import futures 
from google.oauth2 import service_account
from google.cloud import pubsub_v1

url = 'https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id='

PARENT_DIR = os.path.dirname(os.path.dirname(__file__))
SERVICE_ACCOUNT_FILE = os.path.join(PARENT_DIR, "key_pubsub.json") # path to your service account key file you created and downloaded
PROJECT_ID = "data-eng-scalzone"
TOPIC_ID = "my-topic"
VEHICLE_FILE = "/srv/DataEng/vehicles.txt"  # path to vehicle IDs file

class Publisher():
    '''Publisher class to handle publishing messages to a Google Cloud Pub/Sub topic.'''

    def __init__(self, service_account_file, project_id, topic_id):
        self.service_account_file = service_account_file
        self.project_id = project_id
        self.topic_id = topic_id
        self.pubsub_creds = (service_account.Credentials.from_service_account_file(self.service_account_file))
        self.publisher = pubsub_v1.PublisherClient(credentials=self.pubsub_creds)
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)
        
    def future_callback(self, future):
        '''Callback function to handle the result of the publish operation.'''
        try: future.result()              # Wait for the result of the publish operation.
        except Exception as e: print(f"An error occurred: {e}")


    def publish_data(self, vehicle_list_path, url):
        '''Publish data from a list of vehicle IDs to a Google Cloud Pub/Sub topic.'''
        with open(vehicle_list_path, 'r') as cars: vehicles = [bus.strip() for bus in cars.readlines()]
        future_list = [] # This future_list will allow us to create a data structure to inspect in a blocking for loop shortly.
        count = 0 # Track the records.
        for bus in vehicles:
            print(f'GET: Bus {bus}')
            try:
                req = urlopen(url + bus)
                print("opened_file")
                data = json.loads(req.read())
                for item in data:
                    data_str = json.dumps(item)
                    data = data_str.encode("utf-8") # Data must be a bytestring
                    future = self.publisher.publish(self.topic_path, data) # When you publish a message, the client returns a future.
                    future.add_done_callback(self.future_callback) # This is the callback we use to essentially acknowledge the result.
                    future_list.append(future) # Append this new future to our list of futures.
                    count += 1
            except Exception as e: print(e)
        print("Total count: ", count)
        for future in futures.as_completed(future_list):
            continue
        print("All messages published successfully.")
        
my_publisher = Publisher(SERVICE_ACCOUNT_FILE, PROJECT_ID, TOPIC_ID)
my_publisher.publish_data(VEHICLE_FILE, url)
