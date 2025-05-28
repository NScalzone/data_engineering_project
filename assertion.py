import json
import pandas as pd
import re

def filter_date(date_val):
    new_date = re.sub(":[0-9]{2}", "", date_val)
    return new_date  

def filter_act_time(act_time:str):
    next_day = 0
    time_val = float(act_time)
    seconds = time_val % 60
    minutes = (time_val - seconds) // 60
    hours = minutes // 60
    minutes = minutes - (hours * 60)
    check = seconds + (minutes * 60) + (hours * 60 * 60)
    if check != time_val:
        print('math mistake', check)
    hours = '{:02}'.format(int(hours))
    minutes =  '{:02}'.format(int(minutes))
    seconds =  '{:02}'.format(int(seconds))
    if int(hours) >= 24:
        # print("pre math hours is:", hours)
        hours = '{:02}'.format(int(hours) - 24)
        # print("hours is: ", hours)
        next_day = 1
    return[hours, minutes, seconds, next_day]

def get_timestamp(date_val, act_time):
    time = filter_act_time(act_time)
    
    #indicates that it's the next day
    if time[3]:
        day = int(date_val[:2])
        day += 1
        day = str(day)
        date_val = date_val[2:]
        date_val = day + date_val
    date_string = filter_date(date_val)+"T"+time[0]+time[1]+time[2]
    return pd.Timestamp(date_string)

def date_transform(breadcrumb:dict) -> str:
    date = breadcrumb['OPD_DATE']
    time = breadcrumb['ACT_TIME']
    timestamp = get_timestamp(date, time)
    return timestamp

def check_vehicle_id(breadcrumb: dict):
    with open('vehicles.txt', 'r') as cars:
        vehicles = [bus.strip() for bus in cars.readlines()]
    if str(breadcrumb['VEHICLE_ID']) not in vehicles:
        raise ValueError("Invalid vehicle ID")

def check_meters(breadcrumb: dict):
    if breadcrumb['METERS'] < 0:
        raise ValueError("Meters value is negative")

def check_longitude(breadcrumb: dict):
    if breadcrumb['GPS_LONGITUDE'] > -122.0 or breadcrumb['GPS_LONGITUDE'] < -124.0:
        raise ValueError("Longitude out of range")

def check_latitude(breadcrumb: dict):
    if breadcrumb['GPS_LATITUDE'] > 46 or breadcrumb['GPS_LATITUDE'] < 45:
        raise ValueError("Latitude out of range")

def check_hdop_and_satellites(breadcrumb: dict):
    if breadcrumb['GPS_HDOP'] > 20 and breadcrumb['GPS_SATELLITES'] < 2:
        raise ValueError("Poor HDOP with insufficient satellites")

def check_event_no_trip(breadcrumb: dict):
    if not breadcrumb['EVENT_NO_TRIP']:
        raise ValueError("Missing event trip number")

def check_event_no_stop(breadcrumb: dict):
    if not breadcrumb['EVENT_NO_STOP']:
        raise ValueError("Missing event stop number")

def check_opd_date(breadcrumb: dict):
    date_pattern = re.compile(r"^[0-9]{2}[a-zA-Z]{3}[0-9]{4}:[0-9]{2}:[0-9]{2}:[0-9]{2}$")
    if not date_pattern.match(breadcrumb['OPD_DATE']):
        raise ValueError("OPD_DATE format is invalid")

def check_act_time(breadcrumb: dict):
    if breadcrumb['ACT_TIME'] < 0 or breadcrumb['ACT_TIME'] > 172799:
        raise ValueError("ACT_TIME is out of range")

def assertions(breadcrumb: dict) -> bool:
    checks = [
        check_vehicle_id,
        check_meters,
        check_longitude,
        check_latitude,
        check_hdop_and_satellites,
        check_event_no_trip,
        check_event_no_stop,
        check_opd_date,
        check_act_time,
    ]

    for check in checks:
        try:
            check(breadcrumb)
        except Exception as e:
            # Optionally log the error: print(f"{check.__name__} failed: {e}")
            return False

    return True


def inter_record_assertion(breadcrumb_1:dict, breadcrumb_2:dict) -> bool:
    # Redundant datapoint, same time stamp
    if breadcrumb_1['VEHICLE_ID'] == breadcrumb_2['VEHICLE_ID']:
        if breadcrumb_1['ACT_TIME'] == breadcrumb_2['ACT_TIME']:
            return False
        
    return True
