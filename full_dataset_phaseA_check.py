# ============================================
# SE446 - Milestone 2 Full Dataset Cluster Check
# Group 44
#
# Task 10 Evidence:
# This script proves Spark can run on the full HDFS dataset in YARN mode.
# ============================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    spark = (
        SparkSession.builder
        .appName("SE446_M2_Full_Dataset_PhaseA_Check_Group44")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 80)
    print("SE446 Milestone 2 - Full Dataset Cluster Evidence")
    print("=" * 80)
    print("Spark Version:", spark.version)
    print("Spark Master:", spark.sparkContext.master)
    print("Application ID:", spark.sparkContext.applicationId)

    input_path = "hdfs:///data/chicago_crimes.csv"
    print("Input path:", input_path)

    df = spark.read.csv(input_path, header=True, inferSchema=True)

    print("Total rows in full dataset:", df.count())

    print("\nTop 5 crime types:")
    df.groupBy("Primary Type").count().orderBy(col("count").desc()).show(5, truncate=False)

    print("\nTop 5 location hotspots:")
    df.groupBy("Location Description").count().orderBy(col("count").desc()).show(5, truncate=False)

    print("\nArrest distribution:")
    df.groupBy("Arrest").count().orderBy("Arrest").show(truncate=False)

    print("\nfull_dataset_phaseA_check.py completed successfully")

    spark.stop()


if __name__ == "__main__":
    main()
