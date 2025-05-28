# This program loads data into the postgres database using either basic inserts or execute_values()
# Name: Kareem T | Date: 05-11-2025
import time
import os
import psycopg2
import argparse
import json
import pandas as pd
import assertion
import load_breadcrumbs
from psycopg2.extras import execute_values

DBname, DBuser, DBpwd, DBtable, DataDir = "postgres", "postgres", "ps", 'breadcrumb', 'UnspecifiedFile'

# Step 1: DB/table Creation - Handled via pipeline.sql
def DBconnect() -> psycopg2.extensions.connection:
	'''Connect to database'''
	start = time.perf_counter()
	connection = psycopg2.connect(host="localhost", database=DBname, user=DBuser, password=DBpwd,)
	connection.autocommit = False
	if connection: print(f'Database connection successful! in {time.perf_counter() - start:0.4} seconds')
	return connection

# Step 2: Data Loading
def parseCLI() -> tuple:
	'''Parse command line arguments: get the data file name'''
	parser = argparse.ArgumentParser()
	parser.add_argument("-d", "--datadir", required=True)
	parser.add_argument("-t", "--table", required=True)
	global DBtable, DataDir
	DBtable = parser.parse_args().table
	DataDir = parser.parse_args().datadir
	return DataDir, DBtable

def readData(fpath) -> list:
	"""Read all JSON files in the specified directory into a list of breadcrumbs (dictionaries)"""
	start = time.perf_counter()
	with open(fpath) as f: 
		bcrumbs = json.load(f)
		if isinstance(bcrumbs, dict): bcrumbs = [bcrumbs]
	print(f"readData: Read from {fpath} in {time.perf_counter() - start:0.4} seconds")
	return bcrumbs

# Step 3: Data Validation
def assertCrumbs(bcrumbs) -> list: 
	'''Validate that each breadcrumb record satisfies the assertion rules, return valid crumbs'''
	start = time.perf_counter()
	assertion_function = assertion.assertions
	buffer = io.StringIO()
	required_keys = ['EVENT_NO_TRIP', 'EVENT_NO_STOP', 'OPD_DATE', 'VEHICLE_ID', 'METERS', 'ACT_TIME', 'GPS_LONGITUDE', 'GPS_LATITUDE', 'GPS_SATELLITES', 'GPS_HDOP']
	cleaned = []
	for row, crumb in enumerate(bcrumbs):
		if all(crumb.get(key) != None for key in required_keys): # Check if all required keys are None
			if assertion_function(crumb): cleaned.append(crumb) # If crumb is valid, add it to the cleaned list
		# else: print(f"Invalid record {row}: {crumb}") # Print invalid records
		# for key in required_keys: if crumb.get(key) == None: #print(f"Missing key: {key} in record {row_number}, row: {12*row_number} - {crumb}")
	print(f"assertCrumbs: Validating records took {time.perf_counter() - start:0.4} seconds")
	return cleaned


# Step 5: Data Storage
def loadDB(conn, crumbs):
	'''Load data into the database using raw INSERT'''
	with conn.cursor() as cursor:
		start = time.perf_counter()
		buffer = io.StringIO()
		for crumb in crumbs:
			buffer.write(f"{row['id']}\t{row['name']}\t{row['age']}\n")
		buffer.seek(0)
		cursor.copy_from(buffer, DBtable, sep='\t', columns=('id', 'name', 'age'))
		conn.commit()

		print(f'Finished Loading {len(sqlCMDS)} rows. Elapsed Time: {time.perf_counter() - start:0.4} seconds')


def main():
	global DBname, DBuser, DBpwd, DBtable, DataDir
	conn = DBconnect()
	DataDir, DBtable = parseCLI()
	for fname in sorted(os.listdir(DataDir))[5:9]:
		if fname.endswith('.json'):
			print(f"Loading JSON file: {fname}")
			fpath = os.path.join(DataDir, fname)
			data = readData(fpath)#readData(DBfile)
			crumbs = assertCrumbs(data)
			#sqlCMDS = crumbs2SQL(crumbs, DBtable) # Method 1: Slow INSERTs
			#loadDB(conn, sqlCMDS)
			df = pd.DataFrame(crumbs) # <-- Method 2: Faster INSERTs
			tripdf, crumbdf = load_breadcrumbs.process_data(df)
			loadDBFast(tripdf, crumbdf, conn)
		else: print(f"Skipping non-JSON file: {fname}")

if __name__ == "__main__":
	main()