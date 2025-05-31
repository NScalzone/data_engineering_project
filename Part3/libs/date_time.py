import re
import pandas as pd

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
    time = f"{hours}{minutes}{seconds}"
    return time

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
    
