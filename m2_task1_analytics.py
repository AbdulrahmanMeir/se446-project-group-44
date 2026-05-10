from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# ============================================================
# SE446 Big Data Project - Milestone 2
# Task 1: Spark Data Loading + Basic Analytics
# Group 44
# Student: Abdulrahman Meir
#
# This script covers Spark DataFrame analytics and Spark SQL
# analytics required for Milestone 2.
# ============================================================


# ------------------------------------------------------------
# 1. Create Spark Session
# ------------------------------------------------------------
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


# ------------------------------------------------------------
# 2. Load Dataset from HDFS
# ------------------------------------------------------------
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


# ------------------------------------------------------------
# 3. Spark DataFrame Analytics
# ------------------------------------------------------------

# Task 3 - Crime trend over time
print("\n[2] Crimes Per Year")
df.groupBy("Year").count().orderBy("Year").show(30)

# Task 1 - Crime type distribution
print("\n[3] Top 10 Crime Types")
df.groupBy("Primary Type").count().orderBy(col("count").desc()).show(10, truncate=False)

print("\n[4] Arrest Distribution")
df.groupBy("Arrest").count().show()

print("\n[5] Top 10 Districts by Crime Count")
df.groupBy("District").count().orderBy(col("count").desc()).show(10)


# ------------------------------------------------------------
# 4. Spark SQL Analytics
# ------------------------------------------------------------
print("\n[6] Creating Spark SQL Temporary View")

df.createOrReplaceTempView("crimes")

print("Temporary SQL view created: crimes")


# Task 2 - Location hotspots using SQL
print("\n[7] Location Hotspots using Spark SQL")

spark.sql("""
    SELECT
        `Location Description`,
        COUNT(*) AS total_crimes
    FROM crimes
    WHERE `Location Description` IS NOT NULL
    GROUP BY `Location Description`
    ORDER BY total_crimes DESC
    LIMIT 10
""").show(10, truncate=False)


# Task 4 - Overall arrest rate as a proportion between 0 and 1
print("\n[8] Overall Arrest Rate using Spark SQL")

spark.sql("""
    SELECT
        COUNT(*) AS total_records,
        SUM(CASE WHEN Arrest = true THEN 1 ELSE 0 END) AS arrest_count,
        SUM(CASE WHEN Arrest = false THEN 1 ELSE 0 END) AS non_arrest_count,
        ROUND(
            SUM(CASE WHEN Arrest = true THEN 1 ELSE 0 END) * 1.0 / COUNT(*),
            4
        ) AS arrest_rate
    FROM crimes
    WHERE Arrest IS NOT NULL
""").show(truncate=False)


# Task 4 - Arrest rate per crime type as a proportion between 0 and 1
print("\n[9] Arrest Rate by Crime Type using Spark SQL")

spark.sql("""
    SELECT
        `Primary Type`,
        COUNT(*) AS total_records,
        SUM(CASE WHEN Arrest = true THEN 1 ELSE 0 END) AS arrest_count,
        SUM(CASE WHEN Arrest = false THEN 1 ELSE 0 END) AS non_arrest_count,
        ROUND(
            SUM(CASE WHEN Arrest = true THEN 1 ELSE 0 END) * 1.0 / COUNT(*),
            4
        ) AS arrest_rate
    FROM crimes
    WHERE Arrest IS NOT NULL
    GROUP BY `Primary Type`
    ORDER BY arrest_rate DESC
    LIMIT 10
""").show(10, truncate=False)


print("\n[10] Top Crime Types by District using Spark SQL")

spark.sql("""
    SELECT
        District,
        `Primary Type`,
        COUNT(*) AS total_crimes
    FROM crimes
    WHERE District IS NOT NULL
      AND `Primary Type` IS NOT NULL
    GROUP BY District, `Primary Type`
    ORDER BY total_crimes DESC
    LIMIT 15
""").show(15, truncate=False)


# ------------------------------------------------------------
# 5. Interpretation
# ------------------------------------------------------------
print("\nInterpretation:")
print("The Spark DataFrame analytics show the distribution of crimes by year, crime type, arrest status, and district.")
print("THEFT appears as the most frequent crime type in the sample dataset.")
print("The arrest distribution shows strong class imbalance because most records do not result in an arrest.")
print("Spark SQL was used to identify location hotspots and calculate arrest rates.")
print("The arrest rate values are proportions between 0 and 1, which satisfies the Task 4 requirement.")
print("The arrest rate by crime type shows that arrest likelihood differs depending on the crime category.")
print("This supports using crime type, district, hour, and domestic status as features for Spark MLlib models.")


# ------------------------------------------------------------
# 6. Stop Spark Session
# ------------------------------------------------------------
spark.stop()

print("\nTask 1 analytics completed successfully.")