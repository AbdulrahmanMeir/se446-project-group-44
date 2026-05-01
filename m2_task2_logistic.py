from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator


# ============================================================
# SE446 Big Data Project - Milestone 2
# Task 2: Logistic Regression Model
# Group 44
# ============================================================

spark = (
    SparkSession.builder
    .appName("SE446_M2_Task2_LogisticRegression_Group44")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("SE446 Milestone 2 - Task 2: Logistic Regression")
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

lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    maxIter=10,
    regParam=0.1
)

pipeline = Pipeline(stages=[
    primary_type_indexer,
    assembler,
    lr
])

train_data, test_data = df_ml.randomSplit([0.8, 0.2], seed=42)

print("Training rows:", train_data.count())
print("Testing rows:", test_data.count())

print("\n[2] Training Logistic Regression model")
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
print("MODEL RESULTS: Logistic Regression")
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

print("\nInterpretation:")
print("Logistic Regression provides a baseline classification model for predicting arrests.")
print("Because the label is imbalanced, F1 score and AUC should be considered along with accuracy.")
print("If the model predicts mostly non-arrest cases, this reflects the imbalance in the dataset.")

spark.stop()

print("\nTask 2 Logistic Regression completed successfully.")