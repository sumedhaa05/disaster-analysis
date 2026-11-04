"""Step 3 (GIS): interactive heat map of high-impact events. Usage: python gis/make_map.py output/map_points.csv"""
import sys, pandas as pd, folium
from folium.plugins import HeatMap

df = pd.read_csv(sys.argv[1]).dropna(subset=["lat", "lon"])
df = df[(df.lat.between(15, 72)) & (df.lon.between(-180, -60))]  # US + territories bounding box
m = folium.Map(location=[39.5, -98.35], zoom_start=4, tiles="cartodbpositron")
HeatMap(df[["lat", "lon", "casualties"]].fillna(0).values.tolist(), radius=8, blur=12).add_to(m)
m.save("output/hotspot_map.html")
print("Saved output/hotspot_map.html with", len(df), "points")
