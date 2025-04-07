import json
import geojson
from geojson import Feature, FeatureCollection, Point

directory = "/Users/pongabha/Library/CloudStorage/Dropbox/Workspace/BearCat Aviation/Aireon Data/Mongolia Jan 26- Feb 3/2025-01/"
filename = "LogBook-20250126_100.json"

# Load the logbook JSON file
with open(f"{directory}{filename}", 'r') as f:
    logbook_data = json.load(f)

features = []

# Loop through each flight and each hop
for flight in logbook_data.get('flights', []):
    for hop in flight.get('hops', []):
        for event in hop.get('events', []):
            lat = event.get('lat')
            lng = event.get('lng')

            if lat is not None and lng is not None:
                point = Point((lng, lat))
                properties = {
                    "name": event.get("name"),
                    "timestamp": event.get("timestamp"),
                    "date_time": event.get("date_time"),
                    "alt_barometric": event.get("alt_barometric"),
                    "alt_geometric": event.get("alt_geometric"),
                    "speed_ground": event.get("speed_ground"),
                    "heading": event.get("heading"),
                    "airport": event.get("airport"),
                    "aid": event.get("aid"),
                }
                features.append(Feature(geometry=point, properties=properties))

# Create a FeatureCollection and export as GeoJSON
geojson_data = FeatureCollection(features)

with open(f"{directory}logbook.geojson", 'w') as f:
    geojson.dump(geojson_data, f, indent=2)

print("GeoJSON file created: logbook.geojson")

