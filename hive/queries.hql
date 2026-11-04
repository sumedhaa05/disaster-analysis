CREATE DATABASE IF NOT EXISTS disaster;
USE disaster;

DROP TABLE IF EXISTS storm_events;
CREATE EXTERNAL TABLE storm_events (
  event_id BIGINT, state STRING, year INT, month INT, event_type STRING,
  injuries INT, deaths INT, damage_usd DOUBLE, lat DOUBLE, lon DOUBLE, magnitude DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/disaster/curated';

-- Q1: Deadliest and costliest event types
SELECT event_type, COUNT(*) AS events, SUM(deaths) AS deaths,
       SUM(injuries) AS injuries, ROUND(SUM(damage_usd)/1e6,1) AS damage_musd
FROM storm_events GROUP BY event_type ORDER BY deaths DESC LIMIT 10;

-- Q2: Yearly trend
SELECT year, COUNT(*) AS events, SUM(deaths+injuries) AS casualties,
       ROUND(SUM(damage_usd)/1e6,1) AS damage_musd
FROM storm_events GROUP BY year ORDER BY year;

-- Q3: Most affected states
SELECT state, COUNT(*) AS events, SUM(deaths+injuries) AS casualties,
       ROUND(SUM(damage_usd)/1e6,1) AS damage_musd
FROM storm_events GROUP BY state ORDER BY damage_musd DESC LIMIT 15;

-- Q4: Seasonality (month x event type)
SELECT month, event_type, COUNT(*) AS events
FROM storm_events
WHERE event_type IN ('Flood','Flash Flood','Tornado','Hurricane','Wildfire','Excessive Heat','Winter Storm')
GROUP BY month, event_type ORDER BY month, events DESC;
