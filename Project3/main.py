from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from pyspark.ml.feature import VectorAssembler
from models import train_random_forest, train_gbt, evaluate

spark = SparkSession.builder \
    .appName("Cancer Diagnosis Using Machne Learning") \
    .getOrCreate()

info = spark.read.csv("project3_data.csv", header=True, inferSchema=True)
info = info.drop("id")
info = info.withColumn("label", when(col("diagnosis") == "M", 1).otherwise(0)).drop("diagnosis")
features = [c for c in info.columns if c != "label"]
assembler = VectorAssembler(inputCols=features, outputCol="features")
info = assembler.transform(info).select("label", "features")
train, test = info.randomSplit([0.8, 0.2], seed=42)
rf_model = train_random_forest(train)
gbt_model = train_gbt(train)
rf_metrics = evaluate(rf_model, test)
gbt_metrics = evaluate(gbt_model, test)

print("Random Forest Evaluation:")
for k, v in rf_metrics.items():
    print(f"{k}: {v:.4f}")
print("\nBoosting Evaluation:")
for k, v in gbt_metrics.items():
    print(f"{k}: {v:.4f}")
