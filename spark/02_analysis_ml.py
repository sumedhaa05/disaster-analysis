"""Step 2: PySpark analysis + ML. Predicts whether a storm event is 'high impact'
(any casualty or >= $100k damage) from where/when/what type it is.
Usage: spark-submit spark/02_analysis_ml.py <curated_path> <output_dir>
"""
import sys, os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (StructType, StructField, LongType, StringType,
                               IntegerType, DoubleType)
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

curated, out_dir = sys.argv[1], sys.argv[2]
os.makedirs(out_dir, exist_ok=True)
spark = SparkSession.builder.appName("storm-analysis").getOrCreate()

schema = StructType([
    StructField("event_id", LongType()), StructField("state", StringType()),
    StructField("year", IntegerType()), StructField("month", IntegerType()),
    StructField("event_type", StringType()), StructField("injuries", IntegerType()),
    StructField("deaths", IntegerType()), StructField("damage_usd", DoubleType()),
    StructField("lat", DoubleType()), StructField("lon", DoubleType()),
    StructField("magnitude", DoubleType())])
df = spark.read.schema(schema).csv(curated).cache()

# ---- Aggregates for Power BI ----
def save(frame, name):
    frame.toPandas().to_csv(f"{out_dir}/{name}.csv", index=False)

casualties = F.col("injuries") + F.col("deaths")
save(df.groupBy("year", "event_type").agg(F.count("*").alias("events"),
     F.sum(casualties).alias("casualties"), F.sum("damage_usd").alias("damage_usd")), "by_year_type")
save(df.groupBy("state").agg(F.count("*").alias("events"),
     F.sum(casualties).alias("casualties"), F.sum("damage_usd").alias("damage_usd")), "by_state")
save(df.groupBy("month", "event_type").agg(F.count("*").alias("events")), "by_month_type")
save(df.filter(F.col("lat").isNotNull() & ((casualties > 0) | (F.col("damage_usd") > 1e6)))
       .select("event_type", "state", "year", "lat", "lon", casualties.alias("casualties"), "damage_usd")
       .limit(50000), "map_points")

# ---- ML: high-impact classifier ----
ml = (df.filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
        .withColumn("magnitude", F.coalesce("magnitude", F.lit(0.0)))
        .withColumn("label", ((casualties > 0) | (F.col("damage_usd") >= 100000)).cast("double")))

train, test = ml.randomSplit([0.8, 0.2], seed=42)
stages = [
    StringIndexer(inputCols=["event_type", "state"], outputCols=["type_idx", "state_idx"], handleInvalid="keep"),
    OneHotEncoder(inputCols=["type_idx", "state_idx"], outputCols=["type_oh", "state_oh"]),
    VectorAssembler(inputCols=["type_oh", "state_oh", "month", "magnitude", "lat", "lon"], outputCol="features"),
    RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=100, maxDepth=8, seed=42)]
model = Pipeline(stages=stages).fit(train)
pred = model.transform(test)

auc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC").evaluate(pred)
base = test.filter("label = 1").count() / max(test.count(), 1)
print(f"AUC = {auc:.3f} | share of high-impact events in test set = {base:.3f}")
with open(f"{out_dir}/model_metrics.txt", "w") as f:
    f.write(f"AUC={auc:.3f}\nhigh_impact_share={base:.3f}\n")

# Average predicted risk per state and event type, for the dashboard
p1 = F.udf(lambda v: float(v[1]), DoubleType())
risk = (pred.select("state", "event_type", p1("probability").alias("risk_score"))
            .groupBy("state", "event_type")
            .agg(F.avg("risk_score").alias("avg_risk"), F.count("*").alias("n")))
save(risk, "risk_scores")
spark.stop()
