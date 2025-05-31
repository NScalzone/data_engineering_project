#!/usr/bin/python3
import csv, json
from geojson import Feature, FeatureCollection, Point
features = []

with open('q5-3.tsv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter='\t')
    data = csvfile.readlines()
    for line in data[1:len(data)-1]:
        line.strip()
        row = line.split("|")
        if len(row) < 3: continue
        # skip the rows where speed is missing
        #print(row)
        x = row[0].strip()
        y = row[1].strip()
        speed = row[2].strip()
        print(x, y, speed)
        if speed is None or speed == "":
            continue
     
        try:
            latitude, longitude = map(float, (y, x))
            features.append(
                Feature(
                    geometry = Point((longitude,latitude)),
                    properties = {
                        'speed': (int(speed))
                    }
                )
            )
        except ValueError:
            continue

collection = FeatureCollection(features)
with open("q5-3.geojson", "w") as f:
    f.write('%s' % collection)