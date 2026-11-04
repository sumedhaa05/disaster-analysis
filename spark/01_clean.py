"""Step 1: clean raw NOAA Storm Events CSVs and write a compact curated CSV (no header).
Usage: spark-submit spark/01_clean.py <raw_input_path> <curated_output_path>
"""
import sys
from pyspark.sql import SparkSession, functions as F

raw_path, out_path = sys.argv[1], sys.argv[2]
spark = SparkSession.builder.appName("storm-clean").getOrCreate()

df = (spark.read.option("header", True).option("quote", '"').option("escape", '"')
      .csv(raw_path))

def damage_usd(col):
    """NOAA stores damage as strings like '10.00K', '1.5M', '2B'. Convert to dollars."""
    num = F.regexp_extract(F.col(col), r"^([0-9.]+)", 1).cast("double")
    suffix = F.regexp_extract(F.col(col), r"([KMB])$", 1)
    mult = (F.when(suffix == "K", 1e3).when(suffix == "M", 1e6)
             .when(suffix == "B", 1e9).otherwise(1.0))
    return F.coalesce(num * mult, F.lit(0.0))

def to_int(col):
    return F.coalesce(F.col(col).cast("int"), F.lit(0))

clean = (df.select(
            "EVENT_ID", "STATE", "YEAR", "MONTH_NAME", "EVENT_TYPE",
            to_int("INJURIES_DIRECT").alias("inj_d"), to_int("INJURIES_INDIRECT").alias("inj_i"),
            to_int("DEATHS_DIRECT").alias("dth_d"), to_int("DEATHS_INDIRECT").alias("dth_i"),
            (damage_usd("DAMAGE_PROPERTY") + damage_usd("DAMAGE_CROPS")).alias("damage_usd"),
            F.col("BEGIN_LAT").cast("double").alias("lat"),
            F.col("BEGIN_LON").cast("double").alias("lon"),
            F.col("MAGNITUDE").cast("double").alias("magnitude"))
         .withColumn("injuries", F.col("inj_d") + F.col("inj_i"))
         .withColumn("deaths", F.col("dth_d") + F.col("dth_i"))
         .withColumn("month", F.expr(
             "month(to_date(concat('01 ', substring(MONTH_NAME,1,3), ' 2000'), 'dd MMM yyyy'))"))
         .select(F.col("EVENT_ID").alias("event_id"), F.initcap("STATE").alias("state"),
                 F.col("YEAR").cast("int").alias("year"), "month",
                 F.col("EVENT_TYPE").alias("event_type"),
                 "injuries", "deaths", "damage_usd", "lat", "lon", "magnitude")
         .dropna(subset=["event_id", "state", "year", "month", "event_type"])
         .dropDuplicates(["event_id"]))

print("Clean rows:", clean.count())
clean.write.mode("overwrite").option("header", False).csv(out_path)
spark.stop()
