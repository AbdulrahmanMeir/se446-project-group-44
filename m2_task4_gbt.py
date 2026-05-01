from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator


# ============================================================
# SE446 Big Data Project - Milestone 2
# Task 4: Gradient-Boosted Tree Model
# Group 44
# Optimized to avoid cluster memory kill
# ============================================================

spark = (
    SparkSession.builder
    .appName("SE446_M2_Task4_GBT_Group44")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("SE446 Milestone 2 - Task 4: Gradient-Boosted Tree")
print("=" * 80)

print("Spark Version:", spark.version)
print("Spark Master:", spark.sparkContext.master)
print("Application ID:", spark.sparkContext.applicationId)

input_path = "hdfs:///data/chicago_crimes_sample.csv"

print("\n[1] Loading dataset from:", input_path)

df = spark.read.csv(input_path, header=True, inferSchema=True)

df_ml = df.select("Primary Type", "District", "Year", "Domestic", "Arrest")

df_ml = df_ml.filter(col("Arrest").isNotNull())

df_ml = df_ml.fillna({
    "Primary Type": "UNKNOWN",
    "District": -1,
    "Year": -1,
    "Domestic": False
})

df_ml = (
    df_ml
    .withColumn("label", col("Arrest").cast("int"))
    .withColumn("DomesticIndex", col("Domestic").cast("int"))
)

print("Rows after cleaning:", df_ml.count())

print("\nLabel distribution:")
df_ml.groupBy("label").count().show()

primary_type_indexer = StringIndexer(
    inputCol="Primary Type",
    outputCol="PrimaryTypeIndex",
    handleInvalid="keep"
)

assembler = VectorAssembler(
    inputCols=["PrimaryTypeIndex", "District", "Year", "DomesticIndex"],
    outputCol="features",
    handleInvalid="keep"
)

gbt = GBTClassifier(
    featuresCol="features",
    labelCol="label",
    maxIter=3,
    maxDepth=2,
    seed=42
)

pipeline = Pipeline(stages=[
    primary_type_indexer,
    assembler,
    gbt
])

train_data, test_data = df_ml.randomSplit([0.8, 0.2], seed=42)

print("Training rows:", train_data.count())
print("Testing rows:", test_data.count())

print("\n[2] Training Gradient-Boosted Tree model")
model = pipeline.fit(train_data)

print("\n[3] Generating predictions")
predictions = model.transform(test_data)

accuracy_evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="accuracy"
)

f1_evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="f1"
)

precision_evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="weightedPrecision"
)

recall_evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="weightedRecall"
)

auc_evaluator = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)

accuracy = accuracy_evaluator.evaluate(predictions)
f1 = f1_evaluator.evaluate(predictions)
precision = precision_evaluator.evaluate(predictions)
recall = recall_evaluator.evaluate(predictions)
auc = auc_evaluator.evaluate(predictions)

print("\n" + "=" * 80)
print("MODEL RESULTS: Gradient-Boosted Tree")
print("=" * 80)
print(f"Accuracy:  {accuracy:.4f}")
print(f"F1 Score:  {f1:.4f}")
print(f"Precision: {precision:.4f}")
print(f"Recall:    {recall:.4f}")
print(f"AUC:       {auc:.4f}")

print("\nPrediction sample:")
predictions.select(
    "Primary Type",
    "District",
    "Year",
    "label",
    "prediction",
    "probability"
).show(10, truncate=False)

print("\nConfusion Matrix:")
predictions.groupBy("label", "prediction").count().orderBy("label", "prediction").show()

print("\nFeature Importance:")
feature_columns = ["PrimaryTypeIndex", "District", "Year", "DomesticIndex"]
gbt_model = model.stages[-1]
importances = gbt_model.featureImportances.toArray()

for feature, importance in sorted(zip(feature_columns, importances), key=lambda x: x[1], reverse=True):
    print(f"{feature}: {importance:.4f}")

print("\nInterpretation:")
print("Gradient-Boosted Tree is an ensemble model that builds trees sequentially.")
print("The iteration count and depth were reduced to fit the university cluster limits.")
print("Feature importance is used to interpret the strongest prediction factors.")

spark.stop()

print("\nTask 4 Gradient-Boosted Tree completed successfully.")