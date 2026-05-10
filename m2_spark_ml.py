# ============================================
# SE446 - Milestone 2: Spark ML Pipeline
# Group 44
# Student: Abdulrahman Meir
#
# Tasks Covered:
# Task 5: Feature Engineering Pipeline
# Task 6: Train and Evaluate Three Models
# Task 7: Feature Importances & Interpretation
#
# This script is self-contained and designed to run with spark-submit.
# ============================================

import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_timestamp
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import (
    LogisticRegression,
    RandomForestClassifier,
    GBTClassifier,
)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)


def print_section(title):
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)


def evaluate_model(model_name, predictions, training_time):
    """
    Evaluate a trained model using the main metrics required in Milestone 2.
    """

    auc_evaluator = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    )

    accuracy_evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="accuracy",
    )

    f1_evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="f1",
    )

    precision_evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="weightedPrecision",
    )

    recall_evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="weightedRecall",
    )

    auc = auc_evaluator.evaluate(predictions)
    accuracy = accuracy_evaluator.evaluate(predictions)
    f1 = f1_evaluator.evaluate(predictions)
    precision = precision_evaluator.evaluate(predictions)
    recall = recall_evaluator.evaluate(predictions)

    print_section(f"MODEL RESULTS: {model_name}")
    print(f"AUC-ROC:        {auc:.4f}")
    print(f"Accuracy:       {accuracy:.4f}")
    print(f"F1 Score:       {f1:.4f}")
    print(f"Precision:      {precision:.4f}")
    print(f"Recall:         {recall:.4f}")
    print(f"Training Time:  {training_time:.2f} seconds")

    print("\nConfusion Matrix:")
    predictions.groupBy("label", "prediction").count().orderBy(
        "label", "prediction"
    ).show()

    return {
        "model": model_name,
        "auc": auc,
        "accuracy": accuracy,
        "f1": f1,
        "precision": precision,
        "recall": recall,
        "training_time": training_time,
    }


def main():
    spark = (
        SparkSession.builder
        .appName("SE446_M2_Tasks_5_7_Spark_ML_Group44")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print_section("SE446 Milestone 2 - Spark ML Pipeline")
    print("Spark Version:", spark.version)
    print("Spark Master:", spark.sparkContext.master)
    print("Application ID:", spark.sparkContext.applicationId)

    # For Phase B, the official milestone allows using the sample dataset
    # or sampling the full dataset because ML training may exceed cluster memory.
    input_path = "hdfs:///data/chicago_crimes_sample.csv"

    print_section("Loading Dataset")
    print("Input path:", input_path)

    df = spark.read.csv(input_path, header=True, inferSchema=True)

    total_rows = df.count()
    print("Total rows loaded:", total_rows)
    print("Available columns:")
    print(df.columns)

    # ============================================
    # Task 5: Feature Engineering Pipeline
    #
    # Required features:
    # 1. District
    # 2. crime_index from Primary Type
    # 3. Hour extracted from Date
    # 4. domestic_index from Domestic
    #
    # Label:
    # Arrest
    # ============================================

    print_section("Task 5: Feature Engineering Pipeline")

    df_ml = df.select(
        col("Primary Type"),
        col("District"),
        col("Date"),
        col("Domestic"),
        col("Arrest"),
    )

    df_ml = df_ml.filter(col("Arrest").isNotNull())

    # Chicago Crime Date format is usually like:
    # 01/01/2024 12:30:00 AM
    df_ml = df_ml.withColumn(
        "ParsedDate",
        to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a"),
    )

    df_ml = df_ml.withColumn("Hour", hour(col("ParsedDate")))

    # Clean missing values so the ML pipeline does not fail.
    df_ml = df_ml.fillna(
        {
            "Primary Type": "UNKNOWN",
            "District": -1,
            "Hour": -1,
            "Domestic": False,
        }
    )

    df_ml = df_ml.withColumn("label", col("Arrest").cast("integer"))

    # Make Domestic numeric for the feature vector.
    df_ml = df_ml.withColumn("domestic_index", col("Domestic").cast("integer"))

    # Keep only valid binary labels.
    df_ml = df_ml.filter((col("label") == 0) | (col("label") == 1))

    cleaned_rows = df_ml.count()
    print("Rows after cleaning:", cleaned_rows)

    print("\nLabel distribution:")
    df_ml.groupBy("label").count().orderBy("label").show()

    crime_indexer = StringIndexer(
        inputCol="Primary Type",
        outputCol="crime_index",
        handleInvalid="keep",
    )

    assembler = VectorAssembler(
        inputCols=["District", "crime_index", "Hour", "domestic_index"],
        outputCol="features",
        handleInvalid="keep",
    )

    feature_pipeline = Pipeline(stages=[crime_indexer, assembler])

    feature_model = feature_pipeline.fit(df_ml)
    featured_df = feature_model.transform(df_ml)

    print("\nSample feature rows:")
    featured_df.select(
        "Primary Type",
        "District",
        "Hour",
        "Domestic",
        "crime_index",
        "domestic_index",
        "features",
        "label",
    ).show(5, truncate=False)

    print("\nFeature vector explanation:")
    print("features[0] = District")
    print("features[1] = crime_index created from Primary Type")
    print("features[2] = Hour extracted from Date")
    print("features[3] = domestic_index created from Domestic")

    train_data, test_data = featured_df.randomSplit([0.8, 0.2], seed=42)

    train_data = train_data.cache()
    test_data = test_data.cache()

    print("\nTraining rows:", train_data.count())
    print("Testing rows:", test_data.count())

    # ============================================
    # Task 6: Train and Evaluate Three Models
    #
    # Safer cluster parameters are used here because the YARN environment
    # may kill heavy jobs when memory is limited.
    # ============================================

    print_section("Task 6: Train and Evaluate Three Models")

    models = [
        (
            "Logistic Regression",
            LogisticRegression(
                featuresCol="features",
                labelCol="label",
                maxIter=50,
                regParam=0.01,
            ),
        ),
        (
            "Random Forest",
            RandomForestClassifier(
                featuresCol="features",
                labelCol="label",
                numTrees=30,
                maxDepth=5,
                seed=42,
            ),
        ),
        (
            "Gradient-Boosted Trees",
            GBTClassifier(
                featuresCol="features",
                labelCol="label",
                maxIter=20,
                maxDepth=5,
                seed=42,
            ),
        ),
    ]

    results = []
    trained_models = {}

    for model_name, estimator in models:
        print_section(f"Training {model_name}")
        start_time = time.time()

        model = estimator.fit(train_data)

        training_time = time.time() - start_time
        predictions = model.transform(test_data)

        metrics = evaluate_model(model_name, predictions, training_time)

        results.append(metrics)
        trained_models[model_name] = model

    print_section("Three-Model Comparison Table")
    print(
        f"{'Model':<25} {'AUC':<10} {'Accuracy':<10} {'F1':<10} "
        f"{'Precision':<10} {'Recall':<10} {'Time(s)':<10}"
    )

    for result in results:
        print(
            f"{result['model']:<25} "
            f"{result['auc']:<10.4f} "
            f"{result['accuracy']:<10.4f} "
            f"{result['f1']:<10.4f} "
            f"{result['precision']:<10.4f} "
            f"{result['recall']:<10.4f} "
            f"{result['training_time']:<10.2f}"
        )

    best_model = max(results, key=lambda item: item["auc"])

    print("\nBest model based on AUC-ROC:")
    print(best_model["model"], f"with AUC = {best_model['auc']:.4f}")

    # ============================================
    # Task 7: Feature Importances & Interpretation
    # ============================================

    print_section("Task 7: Feature Importances & Interpretation")

    rf_model = trained_models["Random Forest"]

    feature_names = [
        "District",
        "crime_index",
        "Hour",
        "domestic_index",
    ]

    importances = rf_model.featureImportances.toArray()

    print("Random Forest Feature Importances:")
    print(f"{'Feature':<20} {'Importance':<12}")

    for feature_name, importance in zip(feature_names, importances):
        print(f"{feature_name:<20} {importance:<12.6f}")

    most_important_index = int(importances.argmax())
    most_important_feature = feature_names[most_important_index]

    print("\nMost important feature:", most_important_feature)

    print("\nInterpretation:")
    print(
        "The Random Forest model identifies which input variable contributes most "
        "to predicting whether an arrest occurs. If crime_index or District has "
        "high importance, this suggests arrest outcomes are strongly related to "
        "the crime type or the district where the incident happened."
    )
    print(
        "Logistic Regression can perform worse than tree-based models because it "
        "assumes a more linear relationship between the features and the label. "
        "Random Forest and GBT can capture non-linear patterns and interactions "
        "between crime type, district, hour, and domestic status."
    )

    train_data.unpersist()
    test_data.unpersist()

    spark.stop()

    print_section("m2_spark_ml.py completed successfully")


if __name__ == "__main__":
    main()