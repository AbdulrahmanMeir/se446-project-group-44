from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


# ============================================================
# SE446 Big Data Project - Milestone 2
# Task 5: Hyperparameter Tuning using CrossValidator
# Group 44
# Optimized to avoid cluster memory kill
# ============================================================

spark = (
    SparkSession.builder
    .appName("SE446_M2_Task5_CrossValidator_Group44")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("SE446 Milestone 2 - Task 5: CrossValidator Tuning")
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

rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    seed=42
)

pipeline = Pipeline(stages=[
    primary_type_indexer,
    assembler,
    rf
])

train_data, test_data = df_ml.randomSplit([0.8, 0.2], seed=42)

print("Training rows:", train_data.count())
print("Testing rows:", test_data.count())

f1_evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="f1"
)

accuracy_evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="accuracy"
)

param_grid = (
    ParamGridBuilder()
    .addGrid(rf.numTrees, [2, 3])
    .addGrid(rf.maxDepth, [2, 3])
    .build()
)

print("\n[2] Starting CrossValidator")
print("Parameter combinations:", len(param_grid))
print("Folds: 2")

cross_validator = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=param_grid,
    evaluator=f1_evaluator,
    numFolds=2,
    seed=42,
    parallelism=1
)

cv_model = cross_validator.fit(train_data)

print("\n[3] Evaluating tuned model")
predictions = cv_model.transform(test_data)

f1 = f1_evaluator.evaluate(predictions)
accuracy = accuracy_evaluator.evaluate(predictions)

print("\n" + "=" * 80)
print("MODEL RESULTS: Tuned Random Forest with CrossValidator")
print("=" * 80)
print(f"Accuracy: {accuracy:.4f}")
print(f"F1 Score: {f1:.4f}")

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

best_rf_model = cv_model.bestModel.stages[-1]

print("\nBest Parameters:")
print("numTrees:", best_rf_model.getNumTrees)
print("maxDepth:", best_rf_model.getOrDefault("maxDepth"))

print("\nFeature Importance:")
feature_columns = ["PrimaryTypeIndex", "District", "Year", "DomesticIndex"]
importances = best_rf_model.featureImportances.toArray()

for feature, importance in sorted(zip(feature_columns, importances), key=lambda x: x[1], reverse=True):
    print(f"{feature}: {importance:.4f}")

print("\nInterpretation:")
print("CrossValidator tests multiple hyperparameter combinations and selects the model with the best F1 score.")
print("The grid was kept small because the university cluster has limited memory.")
print("This still demonstrates correct Spark ML hyperparameter tuning using CrossValidator.")

spark.stop()

print("\nTask 5 CrossValidator completed successfully.")