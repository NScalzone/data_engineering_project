from bs4 import BeautifulSoup
import pandas as pd
import io
from libs.date_time import filter_act_time
pd.options.mode.chained_assignment = None  # default='warn'


def html_to_df(html_path):
    with open(html_path) as filehandle:
        soup = BeautifulSoup(filehandle, features='html.parser')
        type(soup)

        id_strings = soup.find_all('h2')

        trip_ids = []
        for id in id_strings:
            prefix = 'Stop events for PDX_TRIP '
            text = id.get_text(strip=True)
            if text.startswith(prefix): trip_ids.append(text[len(prefix):])
            else: trip_ids.append(text)

        tables = soup.find_all('table')
        table_dfs = []
        id_index = 0

        for table in tables:
            df = pd.read_html(io.StringIO(str(table)))[0]
            df['trip_id'] = trip_ids[id_index]
            id_index += 1
            table_dfs.append(df)

        whole_table = pd.concat(table_dfs, ignore_index=True)
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
    # print(temp)
    return temp

def get_timestamp(row):
    date_string = f'{row["date"]}T{row["arrive_time_hms"]})'
    return pd.Timestamp(date_string)

def create_table(html_file_path):
    whole_table = html_to_df(html_file_path)
    table = whole_table[["trip_id", "route_number", "vehicle_number","service_key", "direction"]]
    
    return table.drop_duplicates()


# To test, using the sample string here, uncomment the following lines:

# bus_path = "subscriber_stop_events/2025/5/18/3002.html"
# stops_df = create_table(bus_path)
# print(stops_df.head())
