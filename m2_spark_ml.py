from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


# ============================================================
# SE446 Big Data Project - Milestone 2
# Spark ML Pipeline on Chicago Crimes Dataset
# Group 44
# ============================================================


# ------------------------------------------------------------
# 1. Create Spark Session
# ------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("SE446_Milestone2_SparkML_Group44")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("SE446 Milestone 2 - Spark ML Pipeline")
print("=" * 80)
print("Spark Version:", spark.version)
print("Spark Master:", spark.sparkContext.master)
print("Application ID:", spark.sparkContext.applicationId)


# ------------------------------------------------------------
# 2. Load Dataset from HDFS
# ------------------------------------------------------------
# Use sample dataset first for testing.
# For full cluster run, change this to:
# hdfs:///data/chicago_crimes.csv
input_path = "hdfs:///data/chicago_crimes_sample.csv"

print("\n[1] Loading dataset from:", input_path)

df = spark.read.csv(input_path, header=True, inferSchema=True)

print("Total rows loaded:", df.count())
print("Total columns:", len(df.columns))

print("\nSchema:")
df.printSchema()

print("\nSample records:")
df.show(5, truncate=False)


# ------------------------------------------------------------
# 3. Basic Spark Analytics
# ------------------------------------------------------------
print("\n[2] Basic Spark Analytics")

print("\nCrimes per Year:")
df.groupBy("Year").count().orderBy("Year").show(30)

print("\nTop 10 Crime Types:")
df.groupBy("Primary Type").count().orderBy(col("count").desc()).show(10, truncate=False)

print("\nArrest Distribution:")
df.groupBy("Arrest").count().show()

print("\nTop 10 Districts by Crime Count:")
df.groupBy("District").count().orderBy(col("count").desc()).show(10)


# ------------------------------------------------------------
# 4. Data Cleaning and Feature Selection
# ------------------------------------------------------------
print("\n[3] Cleaning and Preparing ML Dataset")

selected_columns = [
    "Primary Type",
    "Location Description",
    "District",
    "Ward",
    "Community Area",
    "Beat",
    "Year",
    "Domestic",
    "Arrest"
]

df_ml = df.select(*selected_columns)

# Remove rows where the label is missing
df_ml = df_ml.filter(col("Arrest").isNotNull())

# Fill missing categorical values
df_ml = df_ml.fillna({
    "Primary Type": "UNKNOWN",
    "Location Description": "UNKNOWN"
})

# Fill missing numeric values
df_ml = df_ml.fillna({
    "District": -1,
    "Ward": -1,
    "Community Area": -1,
    "Beat": -1,
    "Year": -1,
    "Domestic": False
})

# Convert boolean columns to numeric
df_ml = (
    df_ml
    .withColumn("label", col("Arrest").cast("int"))
    .withColumn("DomesticIndex", col("Domestic").cast("int"))
)

print("Rows after cleaning:", df_ml.count())

print("\nLabel distribution:")
df_ml.groupBy("label").count().show()

print("\nPrepared ML data sample:")
df_ml.show(5, truncate=False)


# ------------------------------------------------------------
# 5. Feature Engineering Pipeline
# ------------------------------------------------------------
print("\n[4] Building Feature Engineering Pipeline")

primary_type_indexer = StringIndexer(
    inputCol="Primary Type",
    outputCol="PrimaryTypeIndex",
    handleInvalid="keep"
)

location_indexer = StringIndexer(
    inputCol="Location Description",
    outputCol="LocationIndex",
    handleInvalid="keep"
)

feature_columns = [
    "PrimaryTypeIndex",
    "LocationIndex",
    "District",
    "Ward",
    "Community Area",
    "Beat",
    "Year",
    "DomesticIndex"
]

assembler = VectorAssembler(
    inputCols=feature_columns,
    outputCol="features",
    handleInvalid="keep"
)


# ------------------------------------------------------------
# 6. Train/Test Split
# ------------------------------------------------------------
train_data, test_data = df_ml.randomSplit([0.8, 0.2], seed=42)

print("Training rows:", train_data.count())
print("Testing rows:", test_data.count())


# ------------------------------------------------------------
# 7. Evaluation Function
# ------------------------------------------------------------
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


def evaluate_model(model_name, fitted_model, test_df):
    print("\n" + "=" * 80)
    print(f"MODEL RESULTS: {model_name}")
    print("=" * 80)

    predictions = fitted_model.transform(test_df)

    accuracy = accuracy_evaluator.evaluate(predictions)
    f1 = f1_evaluator.evaluate(predictions)
    precision = precision_evaluator.evaluate(predictions)
    recall = recall_evaluator.evaluate(predictions)
    auc = auc_evaluator.evaluate(predictions)

    print(f"Accuracy:  {accuracy:.4f}")
    print(f"F1 Score:  {f1:.4f}")
    print(f"Precision: {precision:.4f}")
    print(f"Recall:    {recall:.4f}")
    print(f"AUC:       {auc:.4f}")

    print("\nPrediction sample:")
    predictions.select(
        "Primary Type",
        "Location Description",
        "District",
        "Year",
        "label",
        "prediction",
        "probability"
    ).show(10, truncate=False)

    print("\nConfusion Matrix:")
    predictions.groupBy("label", "prediction").count().orderBy("label", "prediction").show()

    return {
        "model": model_name,
        "accuracy": accuracy,
        "f1": f1,
        "precision": precision,
        "recall": recall,
        "auc": auc,
        "predictions": predictions
    }


# ------------------------------------------------------------
# 8. Model 1 - Logistic Regression
# ------------------------------------------------------------
print("\n[5] Training Logistic Regression")

lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    maxIter=20
)

lr_pipeline = Pipeline(stages=[
    primary_type_indexer,
    location_indexer,
    assembler,
    lr
])

lr_model = lr_pipeline.fit(train_data)
lr_results = evaluate_model("Logistic Regression", lr_model, test_data)


# ------------------------------------------------------------
# 9. Model 2 - Random Forest
# ------------------------------------------------------------
print("\n[6] Training Random Forest")

rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    numTrees=50,
    maxDepth=8,
    seed=42
)

rf_pipeline = Pipeline(stages=[
    primary_type_indexer,
    location_indexer,
    assembler,
    rf
])

rf_model = rf_pipeline.fit(train_data)
rf_results = evaluate_model("Random Forest", rf_model, test_data)


# ------------------------------------------------------------
# 10. Model 3 - Gradient-Boosted Tree
# ------------------------------------------------------------
print("\n[7] Training Gradient-Boosted Tree")

gbt = GBTClassifier(
    featuresCol="features",
    labelCol="label",
    maxIter=30,
    maxDepth=5,
    seed=42
)

gbt_pipeline = Pipeline(stages=[
    primary_type_indexer,
    location_indexer,
    assembler,
    gbt
])

gbt_model = gbt_pipeline.fit(train_data)
gbt_results = evaluate_model("Gradient-Boosted Tree", gbt_model, test_data)


# ------------------------------------------------------------
# 11. Feature Importance
# ------------------------------------------------------------
print("\n[8] Feature Importance")

def print_feature_importance(model_name, pipeline_model):
    print("\n" + "-" * 80)
    print(f"Feature Importance: {model_name}")
    print("-" * 80)

    classifier_model = pipeline_model.stages[-1]

    if hasattr(classifier_model, "featureImportances"):
        importances = classifier_model.featureImportances.toArray()

        feature_importance_list = list(zip(feature_columns, importances))
        feature_importance_list = sorted(
            feature_importance_list,
            key=lambda x: x[1],
            reverse=True
        )

        for feature, importance in feature_importance_list:
            print(f"{feature}: {importance:.4f}")
    else:
        print("This model does not provide feature importance directly.")


print_feature_importance("Random Forest", rf_model)
print_feature_importance("Gradient-Boosted Tree", gbt_model)


# ------------------------------------------------------------
# 12. Model Comparison
# ------------------------------------------------------------
print("\n[9] Final Model Comparison")
print("=" * 80)

results = [lr_results, rf_results, gbt_results]

for result in results:
    print(
        f"{result['model']} | "
        f"Accuracy={result['accuracy']:.4f} | "
        f"F1={result['f1']:.4f} | "
        f"Precision={result['precision']:.4f} | "
        f"Recall={result['recall']:.4f} | "
        f"AUC={result['auc']:.4f}"
    )

best_model = max(results, key=lambda x: x["f1"])

print("\nBest model based on F1 Score:")
print(best_model["model"], "with F1 =", round(best_model["f1"], 4))


# ------------------------------------------------------------
# 13. Hyperparameter Tuning using CrossValidator
# ------------------------------------------------------------
print("\n[10] Hyperparameter Tuning with CrossValidator")
print("=" * 80)

rf_tuning = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    seed=42
)

rf_tuning_pipeline = Pipeline(stages=[
    primary_type_indexer,
    location_indexer,
    assembler,
    rf_tuning
])

param_grid = (
    ParamGridBuilder()
    .addGrid(rf_tuning.numTrees, [20, 50])
    .addGrid(rf_tuning.maxDepth, [5, 8])
    .build()
)

cross_validator = CrossValidator(
    estimator=rf_tuning_pipeline,
    estimatorParamMaps=param_grid,
    evaluator=f1_evaluator,
    numFolds=3,
    seed=42,
    parallelism=2
)

cv_model = cross_validator.fit(train_data)
cv_results = evaluate_model("Tuned Random Forest with CrossValidator", cv_model, test_data)

best_rf_model = cv_model.bestModel.stages[-1]

print("\nBest Tuned Random Forest Parameters:")
print("numTrees:", best_rf_model.getNumTrees)
print("maxDepth:", best_rf_model.getOrDefault("maxDepth"))


# ------------------------------------------------------------
# 14. Final Interpretation
# ------------------------------------------------------------
print("\n[11] Final Interpretation")
print("=" * 80)
print("The Spark ML pipeline successfully loaded the Chicago Crimes dataset from HDFS,")
print("cleaned the data, engineered features, trained three classification models,")
print("evaluated them using multiple metrics, and tuned the Random Forest model using CrossValidator.")
print("Because the arrest label is imbalanced, F1 score is more useful than accuracy alone.")
print("Tree-based models also provide feature importance, helping explain which crime attributes")
print("contributed most to the prediction.")

spark.stop()

print("\nSpark job completed successfully.")