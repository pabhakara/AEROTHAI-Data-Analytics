import os
import json
import folium
from folium.plugins import Fullscreen

import webbrowser



# Path to your GeoJSON file
directory = "/Users/pongabha/Library/CloudStorage/Dropbox/Workspace/BearCat Aviation/Aireon Data/Mongolia Jan 26- Feb 3/2025-01/"
geojson_path = os.path.join(directory, "logbook_combined.geojson")

# Load GeoJSON data
with open(geojson_path, "r") as f:
    data = json.load(f)

# Initialize map (centered roughly over Asia)
m = folium.Map(location=[35, 100], zoom_start=4, tiles="cartodbpositron")
Fullscreen().add_to(m)

# Add features to map
for feature in data["features"]:
    geom = feature["geometry"]
    props = feature["properties"]
    feature_type = props.get("type")

    if geom["type"] == "Point" and feature_type == "event":
        lat, lon = geom["coordinates"][1], geom["coordinates"][0]
        popup = folium.Popup(
            f"AID: {props.get('aid')}<br>"
            f"Time: {props.get('date_time')}<br>"
            f"Event: {props.get('event_name')}<br>"
            f"Hop ID: {props.get('hop_id')}",
            max_width=300
        )
        folium.CircleMarker(
            location=[lat, lon],
            radius=4,
            color="green",
            fill=True,
            fill_opacity=0.7,
            popup=popup
        ).add_to(m)

    elif geom["type"] == "LineString" and feature_type == "flight_path":
        coords = [(lat, lon) for lon, lat in geom["coordinates"]]
        popup = folium.Popup(
            f"AID: {props.get('aid', 'N/A')}<br>"
            f"{props.get('departure')} → {props.get('arrival')}<br>"
            f"Distance: {props.get('distance_km', 0):.1f} km<br>"
            f"Hop ID: {props.get('hop_id')}",
            max_width=300
        )
        folium.PolyLine(
            locations=coords,
            color="blue",
            weight=2,
            popup=popup,
            opacity=0.6
        ).add_to(m)

# Save to HTML
map_output = os.path.join(directory, "logbook_map.html")
m.save(map_output)
print(f"✅ Map saved: {map_output}")

# Automatically open in browser
webbrowser.open(f"file://{map_output}")
