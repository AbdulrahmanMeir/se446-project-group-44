from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# ============================================================
# SE446 Big Data Project - Milestone 2
# Task 1: Spark Data Loading + Basic Analytics
# Group 44
# ============================================================

spark = (
    SparkSession.builder
    .appName("SE446_M2_Task1_Analytics_Group44")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("SE446 Milestone 2 - Task 1: Spark Analytics")
print("=" * 80)

print("Spark Version:", spark.version)
print("Spark Master:", spark.sparkContext.master)
print("Application ID:", spark.sparkContext.applicationId)

input_path = "hdfs:///data/chicago_crimes_sample.csv"

print("\n[1] Loading dataset from HDFS:")
print(input_path)

df = spark.read.csv(input_path, header=True, inferSchema=True)

print("\nDataset Summary")
print("-" * 80)
print("Total rows:", df.count())
print("Total columns:", len(df.columns))

print("\nSchema:")
df.printSchema()

print("\nSample Records:")
df.show(5, truncate=False)

print("\n[2] Crimes Per Year")
df.groupBy("Year").count().orderBy("Year").show(30)

print("\n[3] Top 10 Crime Types")
df.groupBy("Primary Type").count().orderBy(col("count").desc()).show(10, truncate=False)

print("\n[4] Arrest Distribution")
df.groupBy("Arrest").count().show()

print("\n[5] Top 10 Districts by Crime Count")
df.groupBy("District").count().orderBy(col("count").desc()).show(10)

print("\nInterpretation:")
print("The analytics show that THEFT is the most frequent crime type in the sample dataset.")
print("Most records do not result in an arrest, showing a strong label imbalance.")
print("Crime counts vary by district, which may help as a feature in machine learning.")

spark.stop()

print("\nTask 1 analytics completed successfully.")