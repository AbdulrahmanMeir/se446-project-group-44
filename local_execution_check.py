# ============================================
# SE446 - Milestone 2 Local Execution Check
# Group 44
#
# Task 9 Evidence:
# This script proves that Spark can run in local mode.
# ============================================

from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder
        .appName("SE446_M2_Local_Execution_Check_Group44")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 80)
    print("SE446 Milestone 2 - Local Execution Evidence")
    print("=" * 80)
    print("Spark Version:", spark.version)
    print("Spark Master:", spark.sparkContext.master)
    print("Application ID:", spark.sparkContext.applicationId)

    sample_data = [
        ("THEFT", 120),
        ("BATTERY", 95),
        ("CRIMINAL DAMAGE", 70),
    ]

    columns = ["Primary Type", "Count"]

    df = spark.createDataFrame(sample_data, columns)

    print("\nSample local Spark DataFrame:")
    df.show()

    print("\nlocal_execution_check.py completed successfully")

    spark.stop()


if __name__ == "__main__":
    main()