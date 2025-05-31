import os
import json

def readData(json_dir) -> list:
	"""Read all JSON files in the specified directory into a list of breadcrumbs (dictionaries)"""
	bcrumbs = []
	dir_contents = sorted(os.listdir(json_dir))
	for dir in dir_contents:
		subdir = os.path.join(json_dir, dir)
		print(f'directory: {dir}')
		if os.path.isdir(subdir):
			records = []
			day_contents = sorted(os.listdir(subdir))
			bus_counts = 0
			output_file = f'output/breadcrumbs_2025-05-{dir}.json'
			for fname in day_contents:
				if fname.endswith('.json'):
					with open(os.path.join(subdir, fname)) as f:
						bus_counts += 1
						if bus_counts % 20 == 0: print(f'Proccessing {fname}')
						content = json.load(f)
						if isinstance(content, dict): records.append(content)
						elif isinstance(content, list): records.extend(content)
			os.makedirs('output', exist_ok=True)
			with open(output_file, 'w') as f:
				json.dump(records, f, indent=4)
			print(f'Processed {bus_counts} files in {dir}')
	return bcrumbs
target_dir = input('Enter the directory to read JSON files from: ')
print(f'Reading data from: {target_dir}')
readData(target_dir)