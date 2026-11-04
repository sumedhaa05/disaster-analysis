# Disaster & Emergency Response Analytics (NOAA Storm Events)
SDG 11 | HDFS, MapReduce, Hive | PySpark, Python, GIS, ML, Power BI

**Goal:** find which disaster types, states and seasons cause the most deaths, injuries and damage,
predict which events are likely to be high-impact, and show it on a dashboard for emergency planners.

## Architecture
NOAA Storm Events CSVs -> HDFS (/disaster/raw) -> PySpark cleaning -> HDFS (/disaster/curated)
  -> MapReduce (event-type totals) + Hive (SQL analysis) + PySpark ML (random forest)
  -> CSV outputs -> Power BI dashboard + folium GIS heat map

## Run order
```bash
# 0. Data (NOAA Storm Events details files, 2019-2023)
bash scripts/download_data.sh
hdfs dfs -mkdir -p /disaster/raw && hdfs dfs -put data/raw/*.gz /disaster/raw/

# 1. Clean with PySpark -> curated CSV in HDFS
spark-submit spark/01_clean.py /disaster/raw /disaster/curated

# 2. MapReduce (Hadoop Streaming)
bash mapreduce/run_mapreduce.sh

# 3. Hive analysis
hive -f hive/queries.hql

# 4. PySpark analytics + ML (writes CSVs into output/)
spark-submit spark/02_analysis_ml.py /disaster/curated output

# 5. GIS map
pip install pandas folium
python gis/make_map.py output/map_points.csv
```
No Hadoop cluster? Replace `/disaster/...` with local folders in steps 1 and 4 (Spark runs in local
mode), and run Hive/MapReduce in a Docker Hadoop image or on a college lab machine.

## Power BI dashboard (Get Data -> Text/CSV for each file in output/)
- Page 1 Overview: cards (total events, deaths, damage), line chart from by_year_type, bar chart of top event types
- Page 2 Geography: map from by_state, plus map_points (lat/lon, bubble size = casualties)
- Page 3 Seasonality: matrix or heat grid from by_month_type (month x event type)
- Page 4 Risk: risk_scores table (state x event type, avg_risk) with slicers; quote model_metrics.txt values

## Report outline
1. Problem and SDG 11 link  2. Dataset  3. Architecture  4. Cleaning  5. MapReduce and Hive results
6. ML model and metrics  7. Dashboard screenshots  8. Findings and limitations  9. Future work
Limitations to mention: reporting bias, many events lack coordinates, the model predicts "high impact"
from location, season and type only, class imbalance.
