# This program loads data into the postgres database using either basic inserts or execute_values()
# Name: Kareem T | Date: 05-11-2025
import time
import os
import psycopg2
import argparse
import json
import pandas as pd
import datetime
import io
from pathlib import Path
import libs.assertion as assertion
from libs.event_file_transform import create_table

DBname, DBuser, DBpwd, DBtable, DataDir = os.getenv('DBNAME'), os.getenv('DBUSER'), os.getenv('DBPASS'), 'breadcrumb', 'crumbs'
FILLROUTE = "-1"
FILLSERVICE = 'Weekday'
FILLDIRECTION = 'Out'
# Step 1: DB/table Creation - Handled via pipeline.sql
def DBconnect() -> psycopg2.extensions.connection:
	'''Connect to database'''
	start = time.perf_counter()
	connection = psycopg2.connect(host="localhost", database=DBname, user=DBuser, password=DBpwd,)
	connection.autocommit = False
	if connection: print(f'Database connection successful! in {time.perf_counter() - start:0.4} seconds')
	return connection

# Step 2: Data Loading
def parseCLI() -> None:
	'''Parse command line arguments: get the data file name'''
	parser = argparse.ArgumentParser()
	parser.add_argument("-d", "--datadir", required=False)
	parser.add_argument("-t", "--table", required=False)
	global DBtable, DataDir
	DBtable = parser.parse_args().table
	DataDir = parser.parse_args().datadir

def readJSON(fpath) -> list:
	"""Read all JSON files in the specified directory into a list of breadcrumbs (dictionaries)"""
	start = time.perf_counter()
	with open(fpath) as f: 
		bcrumbs = json.load(f)
		if isinstance(bcrumbs, dict): bcrumbs = [bcrumbs]
	print(f"readData: Read from {fpath} in {time.perf_counter() - start:0.4} seconds")
	return bcrumbs

def readSTOP(dir) -> pd.DataFrame:
	df = pd.DataFrame(columns=['trip_id', 'route_number', 'vehicle_number', 'service_key', 'direction'])
	for html in sorted(os.listdir(dir)):
		fname = os.path.join(dir, html)
		df = pd.concat([df, create_table(fname)], ignore_index=True)
	print(df.isnull().sum())
	df = df.dropna()
	df = df[~df['trip_id'].duplicated(keep=False)]
	df = df[~(df['trip_id']=='-1')]
	print(f'Pulled {df.shape} stop events from {dir}')
	return df

# Step 3: Data Validation
def assertCrumbs(bcrumbs) -> list: 
	'''Validate that each breadcrumb record satisfies the assertion rules, return valid crumbs'''
	start = time.perf_counter()
	assertion_function = assertion.assertions
	required_keys = ['EVENT_NO_TRIP', 'EVENT_NO_STOP', 'OPD_DATE', 'VEHICLE_ID', 'METERS', 'ACT_TIME', 'GPS_LONGITUDE', 'GPS_LATITUDE', 'GPS_SATELLITES', 'GPS_HDOP']
	cleaned = []
	valid, invalid = 0, 0
	for row, crumb in enumerate(bcrumbs):
		if all(crumb.get(key) != None for key in required_keys): # Check if all required keys are None
			if assertion_function(crumb): 
				cleaned.append(crumb) # If crumb is valid, add it to the cleaned list
				valid += 1
			else: invalid += 1
		else: invalid += 1 # print(f"Invalid record {row}: {crumb}") # Print invalid records
		# for key in required_keys: if crumb.get(key) == None: #print(f"Missing key: {key} in record {row_number}, row: {12*row_number} - {crumb}")
	print(f"assertCrumbs: {valid}/{invalid} (kept/removed). Validating records took {time.perf_counter() - start:0.4} seconds")
	return cleaned

# Step 4: Data Transformation
def process_crumbs(df) -> tuple:
	'''Process the breadcrumb data, calculate speed, and create trip records'''
	df['base_date'] = df['OPD_DATE'].apply(lambda d: datetime.datetime.strptime(d.split(":")[0], "%d%b%Y"))
	df['tstamp'] = df.apply(lambda row: row['base_date'] + datetime.timedelta(seconds=row['ACT_TIME']), axis=1)
	df = df.sort_values(by=['EVENT_NO_TRIP', 'ACT_TIME'])
	df['speed'] = 0.0
	for trip_id, group in df.groupby('EVENT_NO_TRIP'):
		group = group.sort_values('ACT_TIME').copy()
		group['distance_diff'] = group['METERS'].diff()
		group['time_diff'] = group['ACT_TIME'].diff()
		group['speed'] = group['distance_diff'] / group['time_diff']
		if len(group) > 1:
			group.loc[group.index[0], 'speed'] = group['speed'].iloc[1]
		df.loc[group.index, 'speed'] = group['speed']

	trip_records = {}
	for trip_id, group in df.groupby('EVENT_NO_TRIP'):
		first = group.iloc[0]
		trip_records[trip_id] = {
			'trip_id': trip_id,
			'route_id': FILLROUTE, # Fill variable handles missing values
			'vehicle_id': first['VEHICLE_ID'],
			'service_key': FILLSERVICE,
			'direction': FILLDIRECTION}
	trip_df = pd.DataFrame(trip_records.values())
	breadcrumb_df = df[['tstamp', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'speed', 'EVENT_NO_TRIP']].copy()
	breadcrumb_df.columns = ['tstamp', 'latitude', 'longitude', 'speed', 'trip_id']
	breadcrumb_df = breadcrumb_df.dropna()
	breadcrumb_df = breadcrumb_df[~(breadcrumb_df['trip_id']=='-1')]
	print(df.isnull().sum())
	return breadcrumb_df, trip_df

def process_stops(df) -> pd.DataFrame:
	'''Process the stop data, fill missing values, and rename columns'''
	df['service_key'] = df['service_key'].replace({
	'W': 'Weekday',
	'S': 'Saturday',
	'U': 'Sunday',
	'M': 'Weekday'
})
	df['direction'] = df['direction'].replace({
		1: 'Back',
		0: 'Out'
	})
	df = df.rename(columns={'route_number': 'route_id', 'vehicle_number': 'vehicle_id'})
	df = df.drop_duplicates()
	return df

# Step 5: Data Storage
def loadDB(conn, data, table) -> None:
	'''Load data into the database using raw INSERT'''
	with conn.cursor() as cursor:
		if table == 'breadcrumb': 
			columns = ['tstamp', 'latitude', 'longitude', 'speed', 'trip_id']
			data = data.to_dict(orient='records') # Convert from df to json
		elif table == 'trip': 
			columns = ['trip_id', 'route_id', 'vehicle_id', 'service_key', 'direction']
			cursor.execute("SELECT trip_id FROM trip") # Copy_from() does not error-check, we must query existing keys
			existing_ids = set(int(row[0]) for row in cursor.fetchall())
			print(f'Existing trip_ids: {len(data)}')
			data = data[~data['trip_id'].isin(existing_ids)] # Trip_id is a primary key, don't insert record with existing keys
			print(f'New trip_ids: {len(data)}')
			data = data.to_dict(orient='records') # Convert from df to json
		print(f'Loading data into {table}')
		start = time.perf_counter()
		buffer = io.StringIO()
		count = 0
		for crumb in data:
			row = '\t'.join(str(crumb[col]) for col in columns)
			if count % len(data)//4 == 0: print(row)
			buffer.write(row + '\n')
			count += 1
		buffer.seek(0)
		cursor.copy_from(buffer, table, sep='\t', columns=columns)
		conn.commit()
		print(f'Finished Loading {count} rows. Elapsed Time: {time.perf_counter() - start:0.4} seconds')		

def query(querystring) -> list:
	'''Execute a query and return the results'''
	conn = DBconnect()
	with conn.cursor() as cursor:
		cursor.execute(querystring)
		return cursor.fetchall()

# Step 6: Main Functions
def store_pubsub_data(crumbs, table):
	'''Store pubsub data into the database'''
	conn = DBconnect()
	if table == 'breadcrumb':
		crumbs = assertCrumbs(crumbs)
		crumbdf, tripdf = process_crumbs(pd.DataFrame(crumbs))
		loadDB(conn, process_stops(tripdf), table='trip')
		loadDB(conn, crumbdf, table)
	elif table == 'trip':
		df = pd.DataFrame(crumbs)
		df = process_stops(df)
		loadDB(conn, df, table)
	else: raise ValueError(f"Unsupported table: {table}. Supported tables are 'breadcrumb' and 'trip'.")

def main():
	global DBtable, DataDir
	conn = DBconnect()
	parseCLI()
	if DBtable == 'breadcrumb':
		print(f'Storing data into breadcrumb')
		for fname in sorted(os.listdir(DataDir)):
			if fname.endswith('.json'):
				print(f"Loading JSON file: {fname}")
				fpath = os.path.join(DataDir, fname)
				data = readJSON(fpath)
				crumbs = assertCrumbs(data)
				df = pd.DataFrame(crumbs) 
				crumbdf, tripdf = process_crumbs(df)
				loadDB(conn, process_stops(tripdf), table='trip')
				loadDB(conn, crumbdf, DBtable)
			else: print(f"Skipping non-JSON file: {fname}")
		print(f'Complete!')
		return
	elif DBtable == 'trip':
		print(f'Storing data into trip')
		for fname in sorted(os.listdir(DataDir)):
			fpath = os.path.join(DataDir, fname)
			df = readSTOP(fpath)
			df = process_stops(df)
			loadDB(conn, df, DBtable)
	print(os.getenv('DBPASS'))
if __name__ == "__main__": main()