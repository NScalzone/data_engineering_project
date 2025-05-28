from bs4 import BeautifulSoup
import pandas as pd
import io
from date_time import filter_act_time
pd.options.mode.chained_assignment = None  # default='warn'


def html_to_df(html_path):
    filehandle = open(html_path)

    soup = BeautifulSoup(filehandle, features='html.parser')
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
        id_index += 1
        table_dfs.append(df)

    whole_table = table_dfs[0][0]
    for i in range(1,len(table_dfs)):
        whole_table = pd.concat([whole_table, table_dfs[i][0]], axis=0)
    
    date = get_date(soup.find("h1"))
    
    whole_table['date'] = date
        
    return whole_table

def get_date(h1_string):

    temp = str(h1_string)
    # print(temp)
    temp = temp.strip('<h1>')
    # print(temp)
    temp = temp.strip('Trimet CAD/AVL stop data for ')
    # print(temp)
    temp = temp.strip('<')
    return temp

def get_timestamp(row):
    date_string = row["date"] + "T" + str(row["arrive_time_hms"])
    return pd.Timestamp(date_string)

def create_table(html_file_path):
    whole_table = html_to_df(html_file_path)
    table = whole_table[["trip_id", "route_number", "vehicle_number","service_key", "direction"]]
    
    return table


# To test, using the sample string here, uncomment the following lines:

# bus_path = "subscriber_stop_events/2025/5/18/3002.html"
# stops_df = create_table(bus_path)
# print(stops_df.head())
