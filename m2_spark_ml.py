from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("M2_Spark_ML").getOrCreate()

# Load data
df = spark.read.csv("hdfs:///data/chicago_crimes_sample.csv", header=True, inferSchema=True)

# Select columns
df_ml = df.select("Primary Type", "District", "Arrest")

# Encode categorical
indexer = StringIndexer(inputCol="Primary Type", outputCol="PrimaryTypeIndex")
df_indexed = indexer.fit(df_ml).transform(df_ml)

# Assemble features
assembler = VectorAssembler(
    inputCols=["PrimaryTypeIndex", "District"],
    outputCol="features"
)
df_final = assembler.transform(df_indexed)

# Label
df_final = df_final.withColumn("label", col("Arrest").cast("int"))

# Split
train, test = df_final.randomSplit([0.8, 0.2], seed=42)

# Model
lr = LogisticRegression(featuresCol="features", labelCol="label")
model = lr.fit(train)

# Predict
predictions = model.transform(test)

# Evaluate
evaluator = BinaryClassificationEvaluator(labelCol="label")
accuracy = evaluator.evaluate(predictions)

print("Model Accuracy:", accuracy)

spark.stop()