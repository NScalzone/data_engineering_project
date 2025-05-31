import json
import pandas as pd
import io
import os
from urllib.request import urlopen
from bs4 import BeautifulSoup
from concurrent import futures 
from google.oauth2 import service_account
from google.cloud import pubsub_v1

url = 'https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num='

PARENT_DIR = os.path.dirname(os.path.dirname(__file__))
SERVICE_ACCOUNT_FILE = os.path.join(PARENT_DIR, "key_pubsub.json") # path to your service account key file you created and downloaded
VEHICLE_FILE = "/srv/DataEng/vehicles.txt"  # path to vehicle IDs file
PROJECT_ID = "data-eng-scalzone"
TOPIC_ID = "stop_events"

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
        try: future.result() # Wait for the result of the publish operation.
        except Exception as e:
            print(f"An error occurred: {e}")

    def publish_data(self, vehicle_list_path, url):
        '''Publish data from a list of vehicle IDs to a Google Cloud Pub/Sub topic.'''
        with open(vehicle_list_path, 'r') as cars: vehicles = [bus.strip() for bus in cars.readlines()]
        future_list = [] # This future_list will allow us to create a data structure to inspect in a blocking for loop shortly.
        count = 0 # Track the records.
        for bus in vehicles:
            print(f'GET: Bus {bus}')
            try:
                bus_url = url + bus # Use the custom SSL context with urllib
                html = urlopen(bus_url)
                print("opened_file")

                #convert to dataframe, ensuring that records with trip_id < 0 are omitted
                soup = BeautifulSoup(html,features='html.parser')
                type(soup)

                id_strings = soup.find_all('h2')

                trip_ids = []
                for id in id_strings:
                    temp = str(id)
                    temp = temp.strip('<h2>')
                    temp = temp.strip('Stop events for PDX_TRIP ')
                    temp = temp.strip('</h2>')
                    trip_ids.append(temp)

                tables = soup.find_all('table')
                table_dfs = []
                id_index = 0

                for table in tables:
                    temp = str(table)
                    df = pd.read_html(io.StringIO(temp))
                    df[0]['trip_id'] = trip_ids[id_index]
                    if int(trip_ids[id_index]) > 0:
                        table_dfs.append(df)
                    id_index += 1

                whole_table = table_dfs[0][0]
                for i in range(1,len(table_dfs)):
                    whole_table = pd.concat([whole_table, table_dfs[i][0]], axis=0)
                    

                # Transform from full table to trip table columns only
                datatable = whole_table[["trip_id", "route_number", "vehicle_number", "service_key", "direction"]]
                
                # Change from pandas to json before sending to pub/sub
                json_table = datatable.to_json(orient='records')
                data_str = json.dumps(json_table)

                # Data must be a bytestring for Pub/Sub. str.encode() defaults to utf-8.
                data = data_str.encode("utf-8")

                # When you publish a message, the client returns a future.
                future = self.publisher.publish(self.topic_path, data)
            
                # This is the callback we use to essentially acknowledge the result.
                future.add_done_callback(self.future_callback)
            
                # Append this new future to our list of futures.
                future_list.append(future)

                count += 1
                if count % 50 == 0:
                    print(count)

            except Exception as e: print(e)
            
        for future in futures.as_completed(future_list):
            continue

my_publisher = Publisher(SERVICE_ACCOUNT_FILE, PROJECT_ID, TOPIC_ID)
my_publisher.publish_data(VEHICLE_FILE, url)