import os
import json
import geojson
import folium
import webbrowser
from geojson import Feature, FeatureCollection, Point, LineString
from folium.plugins import Fullscreen

# Root directory containing all LogBook-*.json files (with subdirectories)
#root_directory = "/Users/pongabha/Library/CloudStorage/Dropbox/Workspace/BearCat Aviation/Aireon Data/Mongolia Jan 26- Feb 3/"
root_directory = "/Users/pongabha/Library/CloudStorage/Dropbox/Workspace/BearCat Aviation/Aireon Data"
# --- Walk through all subdirectories to collect files ---
logbook_files = []
for dirpath, dirnames, filenames in os.walk(root_directory):
    for filename in filenames:
        if filename.startswith("LogBook-") and filename.endswith(".json"):
            full_path = os.path.join(dirpath, filename)
            logbook_files.append(full_path)

print(f"✅ Found {len(logbook_files)} LogBook files.")

# --- Process each file ---
for filepath in logbook_files:
    print(f"\U0001F4E6 Processing: {filepath}")
    with open(filepath, 'r') as f:
        logbook_data = json.load(f)

    features = []
    for flight in logbook_data.get('flights', []):
        for hop in flight.get('hops', []):
            hop_id = hop.get("hop_id")
            coordinates = []

            for event in hop.get('events', []):
                lat = event.get('lat')
                lng = event.get('lng')
                if lat is not None and lng is not None:
                    coordinates.append((lng, lat))
                    point = Point((lng, lat))
                    properties = {
                        "type": "event",
                        "hop_id": hop_id,
                        "event_name": event.get("name"),
                        "timestamp": event.get("timestamp"),
                        "date_time": event.get("date_time"),
                        "alt_barometric": event.get("alt_barometric"),
                        "alt_geometric": event.get("alt_geometric"),
                        "speed_ground": event.get("speed_ground"),
                        "heading": event.get("heading"),
                        "aid": event.get("aid"),
                        "airport": event.get("airport"),
                    }
                    features.append(Feature(geometry=point, properties=properties))

            if len(coordinates) >= 2:
                line = LineString(coordinates)
                line_properties = {
                    "type": "flight_path",
                    "hop_id": hop_id,
                    "departure": hop.get("departure"),
                    "departure_iata": hop.get("departure_iata_code"),
                    "departure_time": hop.get("start_date_time"),
                    "arrival": hop.get("arrival"),
                    "arrival_iata": hop.get("arrival_iata_code"),
                    "arrival_time": hop.get("stop_date_time"),
                    "distance_km": hop.get("distance"),
                    "airborne": hop.get("airborne"),
                    "cruising_time": hop.get("cruising_time"),
                    "climbing_time": hop.get("climbing_time"),
                    "descending_time": hop.get("descending_time"),
                    "turning_time": hop.get("turning_time"),
                }
                features.append(Feature(geometry=line, properties=line_properties))

    # Save GeoJSON
    output_geojson = filepath.replace(".json", ".geojson")
    geojson_data = FeatureCollection(features)
    with open(output_geojson, 'w') as f:
        geojson.dump(geojson_data, f, indent=2)
    print(f"✅ GeoJSON file created: {output_geojson}")

    # --- Create Folium Map ---
    output_html = output_geojson.replace(".geojson", ".html")
    m = folium.Map(location=[30, 100], zoom_start=4, tiles="cartodbpositron")
    Fullscreen().add_to(m)

    for feature in geojson_data["features"]:
        geom = feature["geometry"]
        props = feature["properties"]

        if geom["type"] == "Point":
            lat, lon = geom["coordinates"][1], geom["coordinates"][0]
            folium.CircleMarker(
                location=[lat, lon],
                radius=3,
                color="green",
                fill=True,
                fill_opacity=0.7,
                popup=f"{props.get('aid', '')} | {props.get('event_name', '')} @ {props.get('date_time', '')}"
            ).add_to(m)

        elif geom["type"] == "LineString":
            coords = [(lat, lon) for lon, lat in geom["coordinates"]]
            folium.PolyLine(
                locations=coords,
                color="blue",
                weight=2,
                opacity=0.6,
                popup=f"{props.get('departure')} → {props.get('arrival')} | {props.get('hop_id')}"
            ).add_to(m)

    m.save(output_html)
    print(f"✅ Map saved: {output_html}")
    webbrowser.open(f"file://{output_html}")
