# SE446 Big Data Engineering Project - Group 44

## Chicago Crime Analytics using Hadoop MapReduce and Spark MLlib

### Student / Group Information

**Course:** SE446 - Big Data Engineering  
**Project:** Chicago Crime Analytics  
**Group:** Group 44  
**Student:** Abdulrahman Meir  
**Repository:** `se446-project-group-44`

---

# 1. Project Overview

This project analyzes the Chicago Crimes dataset using big data tools and distributed computing techniques. The project is divided into two main milestones.

- **Milestone 1:** Hadoop MapReduce analytics using Python mapper and reducer scripts.
- **Milestone 2:** Apache Spark analytics and Spark MLlib machine learning pipeline for arrest prediction.

The main goal of the project is to demonstrate the ability to process, analyze, and model large-scale crime data using a distributed Hadoop/Spark cluster environment.

---

# 2. Dataset

The dataset used in this project is the Chicago Crimes dataset stored in HDFS on the university Hadoop/Spark cluster.

## HDFS Dataset Paths

```bash
hdfs:///data/chicago_crimes.csv
hdfs:///data/chicago_crimes_sample.csv
```

For the machine learning phase, the sample dataset was used:

```bash
hdfs:///data/chicago_crimes_sample.csv
```

For full dataset cluster evidence, the full dataset was also processed using Spark on YARN:

```bash
hdfs:///data/chicago_crimes.csv
```

## Dataset Summary

From the Spark ML execution logs using the sample dataset:

- Total rows: **10,000**
- Total columns: **22**
- Spark version: **3.5.4**
- Spark master: **yarn**

From the full dataset YARN evidence:

- Full dataset path: `hdfs:///data/chicago_crimes.csv`
- Total rows in full dataset: **793,073**
- Spark master: **yarn**

The dataset includes fields such as:

- ID
- Case Number
- Date
- Block
- IUCR
- Primary Type
- Description
- Location Description
- Arrest
- Domestic
- Beat
- District
- Ward
- Community Area
- Year
- Latitude
- Longitude

---

# 3. Repository Structure

```text
se446-project-group-44/
│
├── README.md
├── test_data.csv
│
├── src/
│   ├── mapper_task2.py
│   ├── mapper_task3.py
│   ├── mapper_task4.py
│   ├── mapper_task5.py
│   └── reducer_sum.py
│
├── scripts/
│   ├── run_task2.sh
│   ├── run_task3.sh
│   ├── run_task4.sh
│   └── run_task5.sh
│
├── output/
│   ├── task2.txt
│   ├── task3.txt
│   ├── task4.txt
│   ├── task5.txt
│   │
│   └── milestone2/
│       ├── task1_analytics_output.txt
│       ├── task2_logistic_output.txt
│       ├── task3_random_forest_output.txt
│       ├── task4_gbt_output.txt
│       ├── task5_crossvalidator_output.txt
│       ├── m2_spark_ml_client_output.txt
│       ├── local_execution_output.txt
│       └── full_dataset_phaseA_yarn_output.txt
│
├── M2_Spark_ML_Group44.ipynb
├── m2_spark_ml.py
├── m2_full_pipeline_attempt.py
├── m2_task1_analytics.py
├── m2_task2_logistic.py
├── m2_task3_random_forest.py
├── m2_task4_gbt.py
├── m2_task5_crossvalidator.py
├── local_execution_check.py
└── full_dataset_phaseA_check.py
```

---

# 4. Milestone 1 - Hadoop MapReduce

## 4.1 Objective

Milestone 1 focused on using Hadoop Streaming with Python mapper and reducer scripts to analyze the Chicago Crimes dataset stored in HDFS.

The MapReduce jobs were executed on the university Hadoop cluster using Hadoop Streaming.

## 4.2 Milestone 1 Files

Mapper and reducer files are stored in:

```text
src/
```

Execution scripts are stored in:

```text
scripts/
```

Output files are stored in:

```text
output/
```

## 4.3 Milestone 1 Output Files

```text
output/task2.txt
output/task3.txt
output/task4.txt
output/task5.txt
```

These files contain the results of the Hadoop MapReduce jobs.

---

# 5. Milestone 2 - Spark MLlib

## 5.1 Objective

Milestone 2 focuses on using Apache Spark and Spark MLlib to perform distributed data analytics and machine learning on the Chicago Crimes dataset.

The machine learning objective is to predict whether a crime record results in an arrest using selected features from the dataset.

The target label is:

```text
Arrest
```

The label was converted into a binary numeric column:

```text
label = 1 if Arrest = true
label = 0 if Arrest = false
```

---

# 6. Milestone 2 Task Coverage Summary

| Milestone 2 Requirement | Evidence in Repository | Status |
|---|---|---|
| Tasks 1–4: Spark analytics | `m2_task1_analytics.py`, `output/milestone2/task1_analytics_output.txt` | Completed |
| Task 5: Feature engineering pipeline | `m2_spark_ml.py`, `output/milestone2/m2_spark_ml_client_output.txt` | Completed |
| Task 6: Train and evaluate three models | `m2_spark_ml.py`, Logistic Regression, Random Forest, and GBT results | Completed |
| Task 7: Feature importance and interpretation | Random Forest feature importance in `m2_spark_ml.py` output | Completed |
| Task 9: Local execution evidence | `local_execution_check.py`, `output/milestone2/local_execution_output.txt` | Completed |
| Task 10: Cluster/YARN full dataset evidence | `full_dataset_phaseA_check.py`, `output/milestone2/full_dataset_phaseA_yarn_output.txt` | Completed |
| Task 11: Spark-submit execution evidence | `m2_spark_ml.py` executed with `spark-submit --master yarn` | Completed |

---

# 7. Milestone 2 Design Decision

A full Spark ML pipeline was initially attempted in:

```text
m2_full_pipeline_attempt.py
```

However, the university cluster has limited memory. Running multiple ML models and hyperparameter tuning as one large job can cause YARN to terminate the job.

To make the project reliable, the repository includes two types of implementation evidence:

## 7.1 Standalone Final Pipeline

The final standalone ML pipeline is:

```text
m2_spark_ml.py
```

This script covers:

- Task 5: Feature engineering pipeline
- Task 6: Three-model training and evaluation
- Task 7: Feature importance and interpretation
- Task 11: Spark-submit execution evidence

## 7.2 Separate Model Scripts

The repository also keeps separate scripts for additional evidence and safer independent execution:

```text
m2_task1_analytics.py
m2_task2_logistic.py
m2_task3_random_forest.py
m2_task4_gbt.py
m2_task5_crossvalidator.py
```

This design makes the work easier to verify and avoids memory termination on the cluster.

---

# 8. Milestone 2 Scripts

## Task 1 - Spark Analytics

**File:** `m2_task1_analytics.py`

This script loads the Chicago Crimes dataset from HDFS and performs Spark DataFrame analytics.

It includes:

- Dataset loading from HDFS
- Schema inspection
- Sample record display
- Crimes per year
- Top crime types
- Arrest distribution
- District-level crime counts

## Task 5–7 and Task 11 - Standalone Spark ML Pipeline

**File:** `m2_spark_ml.py`

This is the final standalone Spark ML pipeline.

It includes:

- Data loading from HDFS
- Data cleaning
- Hour extraction from the Date column
- StringIndexer for crime type
- VectorAssembler for required feature vector
- Logistic Regression training
- Random Forest training
- Gradient-Boosted Trees training
- Model metrics
- Confusion matrices
- Three-model comparison table
- Feature importance interpretation
- Spark-submit YARN execution evidence

## Additional Model Evidence Scripts

The following scripts provide additional independent model runs:

```text
m2_task2_logistic.py
m2_task3_random_forest.py
m2_task4_gbt.py
m2_task5_crossvalidator.py
```

These scripts were useful because running each model separately reduces memory pressure on the cluster.

## Local Execution Check

**File:** `local_execution_check.py`

This script proves that Spark can run in local mode:

```text
Spark Master: local[*]
```

## Full Dataset YARN Check

**File:** `full_dataset_phaseA_check.py`

This script proves that Spark can read and process the full HDFS dataset using YARN:

```text
hdfs:///data/chicago_crimes.csv
```

---

# 9. Corrected Machine Learning Feature Engineering

The final standalone Spark ML script uses the required Milestone 2 feature vector:

```text
features[0] = District
features[1] = crime_index created from Primary Type
features[2] = Hour extracted from Date
features[3] = domestic_index created from Domestic
```

The previous implementation used `Year`, but the corrected final implementation extracts `Hour` from the `Date` column and uses it in the feature vector.

## Feature Engineering Steps

- `Primary Type` is converted into `crime_index` using `StringIndexer`.
- `Hour` is extracted from the `Date` column using Spark timestamp functions.
- `Domestic` is converted into `domestic_index`.
- `District`, `crime_index`, `Hour`, and `domestic_index` are assembled into the final `features` vector.

Sample feature vector output:

```text
[10.0,12.0,3.0,0.0]
[11.0,10.0,16.0,0.0]
[14.0,7.0,9.0,0.0]
[1.0,25.0,10.0,0.0]
[1.0,2.0,17.0,0.0]
```

---

# 10. Label Distribution

The sample dataset has an imbalanced label distribution:

```text
label = 0 (No Arrest): 8717
label = 1 (Arrest):    1283
```

This means most records did not result in an arrest.

Because of this imbalance, accuracy alone is not enough to evaluate the models. F1 score, precision, recall, AUC, and the confusion matrix are also reported.

---

# 11. Spark Submit Commands

All Milestone 2 tasks were executed on the cluster using `spark-submit`.

The following configuration was used to force Spark to use Python 3.10 because Python 3.12 on the cluster did not have the required NumPy dependency for Spark MLlib.

## 11.1 Final Standalone ML Pipeline

```bash
PYSPARK_PYTHON=/usr/bin/python3.10 \
PYSPARK_DRIVER_PYTHON=/usr/bin/python3.10 \
spark-submit \
  --master yarn \
  --deploy-mode client \
  --driver-memory 512m \
  --executor-memory 1g \
  --executor-cores 1 \
  --num-executors 1 \
  --conf spark.driver.maxResultSize=128m \
  m2_spark_ml.py | tee output/milestone2/m2_spark_ml_client_output.txt
```

## 11.2 Local Execution Check

```bash
PYSPARK_PYTHON=/usr/bin/python3.10 \
PYSPARK_DRIVER_PYTHON=/usr/bin/python3.10 \
spark-submit \
  --master "local[*]" \
  local_execution_check.py | tee output/milestone2/local_execution_output.txt
```

## 11.3 Full Dataset YARN Evidence

```bash
PYSPARK_PYTHON=/usr/bin/python3.10 \
PYSPARK_DRIVER_PYTHON=/usr/bin/python3.10 \
spark-submit \
  --master yarn \
  --deploy-mode client \
  --driver-memory 512m \
  --executor-memory 1g \
  --executor-cores 1 \
  --num-executors 1 \
  --conf spark.driver.maxResultSize=128m \
  full_dataset_phaseA_check.py | tee output/milestone2/full_dataset_phaseA_yarn_output.txt
```

## 11.4 Separate Model Scripts

```bash
spark-submit \
  --conf spark.pyspark.python=/usr/bin/python3.10 \
  --conf spark.pyspark.driver.python=/usr/bin/python3.10 \
  --conf spark.ui.showConsoleProgress=false \
  m2_task1_analytics.py 2>&1 | tee output/milestone2/task1_analytics_output.txt
```

```bash
spark-submit \
  --conf spark.pyspark.python=/usr/bin/python3.10 \
  --conf spark.pyspark.driver.python=/usr/bin/python3.10 \
  --conf spark.ui.showConsoleProgress=false \
  m2_task2_logistic.py 2>&1 | tee output/milestone2/task2_logistic_output.txt
```

```bash
spark-submit \
  --conf spark.pyspark.python=/usr/bin/python3.10 \
  --conf spark.pyspark.driver.python=/usr/bin/python3.10 \
  --conf spark.ui.showConsoleProgress=false \
  m2_task3_random_forest.py 2>&1 | tee output/milestone2/task3_random_forest_output.txt
```

```bash
spark-submit \
  --conf spark.pyspark.python=/usr/bin/python3.10 \
  --conf spark.pyspark.driver.python=/usr/bin/python3.10 \
  --conf spark.ui.showConsoleProgress=false \
  m2_task4_gbt.py 2>&1 | tee output/milestone2/task4_gbt_output.txt
```

```bash
spark-submit \
  --conf spark.pyspark.python=/usr/bin/python3.10 \
  --conf spark.pyspark.driver.python=/usr/bin/python3.10 \
  --conf spark.ui.showConsoleProgress=false \
  m2_task5_crossvalidator.py 2>&1 | tee output/milestone2/task5_crossvalidator_output.txt
```

---

# 12. Output Logs

All Spark-submit outputs were saved inside:

```text
output/milestone2/
```

The output log files are:

```text
task1_analytics_output.txt
task2_logistic_output.txt
task3_random_forest_output.txt
task4_gbt_output.txt
task5_crossvalidator_output.txt
m2_spark_ml_client_output.txt
local_execution_output.txt
full_dataset_phaseA_yarn_output.txt
```

These files include terminal output, Spark application information, dataset loading proof, model results, confusion matrices, feature importance, and completion messages.

---

# 13. Cluster Execution Evidence

The tasks were executed on the university Spark cluster using YARN.

Example evidence from the final standalone ML output:

```text
Spark Version: 3.5.4
Spark Master: yarn
Application ID: application_1777830883738_0060
Input path: hdfs:///data/chicago_crimes_sample.csv
Total rows loaded: 10000
m2_spark_ml.py completed successfully
```

This confirms that the code was executed on the Spark cluster using YARN.

---

# 14. Local Execution Evidence

Local Spark execution was verified using:

```text
local_execution_check.py
```

Output file:

```text
output/milestone2/local_execution_output.txt
```

Important evidence:

```text
Spark Version: 3.5.4
Spark Master: local[*]
Application ID: local-1778420218938
local_execution_check.py completed successfully
```

This confirms that Spark was successfully executed in local mode.

---

# 15. Full Dataset YARN Evidence

Full dataset cluster execution was verified using:

```text
full_dataset_phaseA_check.py
```

Output file:

```text
output/milestone2/full_dataset_phaseA_yarn_output.txt
```

Important evidence:

```text
Spark Version: 3.5.4
Spark Master: yarn
Application ID: application_1777830883738_0061
Input path: hdfs:///data/chicago_crimes.csv
Total rows in full dataset: 793073
full_dataset_phaseA_check.py completed successfully
```

The full dataset run also produced top crime types, top location hotspots, and arrest distribution.

## Full Dataset Results

### Top 5 Crime Types

```text
THEFT              162688
BATTERY            151930
CRIMINAL DAMAGE     91241
NARCOTICS           74127
ASSAULT             54070
```

### Top 5 Location Hotspots

```text
STREET       248326
RESIDENCE    136393
APARTMENT     61235
SIDEWALK      47506
OTHER         29671
```

### Arrest Distribution

```text
false: 571140
true:  221932
NULL:       1
```

This proves that Spark successfully processed the full HDFS dataset in YARN mode.

---

# 16. Milestone 2 Spark Analytics Results

From `m2_task1_analytics.py` using the sample dataset:

## Dataset Summary

```text
Total rows: 10000
Total columns: 22
```

## Top Crime Types

```text
THEFT                  2054
BATTERY                1728
CRIMINAL DAMAGE        1062
MOTOR VEHICLE THEFT     948
ASSAULT                 878
DECEPTIVE PRACTICE      799
OTHER OFFENSE           586
ROBBERY                 508
BURGLARY                316
WEAPONS VIOLATION       284
```

## Arrest Distribution

```text
Arrest = true:   1283
Arrest = false:  8717
```

## Interpretation

The analytics show that theft is the most frequent crime type in the sample dataset. The arrest distribution shows a strong class imbalance because most records do not result in an arrest.

---

# 17. Milestone 1 vs Milestone 2 Comparison

Milestone 1 used Hadoop MapReduce, while Milestone 2 used Spark DataFrames and Spark MLlib.

The purpose of this comparison is to show that Spark reproduces the same type of analytics as MapReduce while also supporting machine learning.

| Analysis | Milestone 1 Approach | Milestone 2 Approach | Result |
|---|---|---|---|
| Crime type distribution | Hadoop MapReduce mapper/reducer | Spark DataFrame `groupBy("Primary Type").count()` | Completed |
| Location hotspots | Hadoop MapReduce mapper/reducer | Spark DataFrame `groupBy("Location Description").count()` | Completed |
| Yearly crime trends | Hadoop MapReduce mapper/reducer | Spark DataFrame grouping by year | Completed |
| Arrest analysis | Hadoop MapReduce mapper/reducer | Spark DataFrame `groupBy("Arrest").count()` | Completed |

## Sample Side-by-Side Results

| Metric | Milestone 1 MapReduce Result | Milestone 2 Spark Result | Match Status |
|---|---:|---:|---|
| THEFT count | 2054 | 2054 | Matches |
| BATTERY count | 1728 | 1728 | Matches |
| CRIMINAL DAMAGE count | 1062 | 1062 | Matches |
| Arrest = true | 1283 | 1283 | Matches |
| Arrest = false | 8717 | 8717 | Matches |

The matching results show that the Spark implementation is consistent with the MapReduce analytics while providing a more flexible API for additional ML tasks.

---

# 18. Final Three-Model ML Results

The final standalone script `m2_spark_ml.py` trained and evaluated three models on the Chicago Crimes sample dataset using Spark MLlib.

| Model | AUC | Accuracy | F1 Score | Precision | Recall | Training Time |
|---|---:|---:|---:|---:|---:|---:|
| Logistic Regression | 0.6530 | 0.8626 | 0.8061 | 0.8029 | 0.8626 | 16.12s |
| Random Forest | 0.7370 | 0.8844 | 0.8529 | 0.8755 | 0.8844 | 16.75s |
| Gradient-Boosted Trees | 0.7423 | 0.8803 | 0.8518 | 0.8618 | 0.8803 | 133.66s |

Based on AUC-ROC, the best model was:

```text
Gradient-Boosted Trees with AUC = 0.7423
```

---

# 19. Confusion Matrix Results

## Logistic Regression

```text
label=0, prediction=0.0: 1651
label=0, prediction=1.0: 9
label=1, prediction=0.0: 255
label=1, prediction=1.0: 6
```

## Random Forest

```text
label=0, prediction=0.0: 1647
label=0, prediction=1.0: 13
label=1, prediction=0.0: 209
label=1, prediction=1.0: 52
```

## Gradient-Boosted Trees

```text
label=0, prediction=0.0: 1635
label=0, prediction=1.0: 25
label=1, prediction=0.0: 205
label=1, prediction=1.0: 56
```

## Interpretation

The Gradient-Boosted Trees model achieved the highest AUC-ROC. Random Forest achieved the highest accuracy and strong F1 score. Logistic Regression was faster but weaker at identifying arrest cases because it assumes a more linear relationship between features and the label.

---

# 20. Feature Importance

The Random Forest model produced the following feature importances:

| Feature | Importance |
|---|---:|
| District | 0.043290 |
| crime_index | 0.885660 |
| Hour | 0.043673 |
| domestic_index | 0.027377 |

The most important feature was:

```text
crime_index
```

## Interpretation

The type of crime was the strongest predictor of whether an arrest occurred. This is reasonable because some crime categories are more likely to lead to arrests than others.

District and Hour contributed less, but they still provide context about where and when the crime occurred.

---

# 21. CrossValidator Results

The CrossValidator script tested a small parameter grid because the university cluster has limited memory.

It demonstrates the use of:

- `ParamGridBuilder`
- `CrossValidator`
- F1-score based evaluation
- Best parameter reporting
- Tuned model evaluation

The grid was intentionally kept small to avoid YARN memory termination.

---

# 22. Challenges and Solutions

## Challenge 1: Python 3.12 Missing NumPy

The default PySpark environment used Python 3.12, but Spark MLlib required NumPy, which was not available for Python 3.12 on the cluster.

### Solution

The Spark jobs were submitted with Python 3.10:

```bash
PYSPARK_PYTHON=/usr/bin/python3.10
PYSPARK_DRIVER_PYTHON=/usr/bin/python3.10
```

or:

```bash
--conf spark.pyspark.python=/usr/bin/python3.10
--conf spark.pyspark.driver.python=/usr/bin/python3.10
```

Python 3.10 had the required NumPy package installed.

## Challenge 2: Full Pipeline Memory Termination

The first full Spark ML pipeline attempted to train multiple models and CrossValidator in one script. The cluster could terminate heavy jobs because of memory limitations.

### Solution

The final repository includes:

- a standalone Spark ML script with stable parameters,
- separate model scripts for safer execution,
- saved output logs for verification,
- sample dataset ML training,
- full dataset YARN evidence through a separate analytics check.

## Challenge 3: Class Imbalance

The dataset is imbalanced:

```text
No Arrest: 8717
Arrest:    1283
```

This caused some models to predict mostly non-arrest cases.

### Solution

Multiple metrics were reported:

- Accuracy
- F1 Score
- Precision
- Recall
- AUC
- Confusion Matrix

This gives a more honest evaluation than accuracy alone.

---

# 23. How to Reproduce Milestone 2

## Step 1: Connect to the Cluster

```bash
ssh ameir@134.209.172.50
```

## Step 2: Enter the Repository

```bash
cd ~/se446-project-group-44
git checkout m2-final-polish
git pull
```

## Step 3: Create Output Folder

```bash
mkdir -p output/milestone2
```

## Step 4: Run the Final Standalone ML Pipeline

```bash
PYSPARK_PYTHON=/usr/bin/python3.10 \
PYSPARK_DRIVER_PYTHON=/usr/bin/python3.10 \
spark-submit \
  --master yarn \
  --deploy-mode client \
  --driver-memory 512m \
  --executor-memory 1g \
  --executor-cores 1 \
  --num-executors 1 \
  --conf spark.driver.maxResultSize=128m \
  m2_spark_ml.py | tee output/milestone2/m2_spark_ml_client_output.txt
```

## Step 5: Run Local Execution Check

```bash
PYSPARK_PYTHON=/usr/bin/python3.10 \
PYSPARK_DRIVER_PYTHON=/usr/bin/python3.10 \
spark-submit \
  --master "local[*]" \
  local_execution_check.py | tee output/milestone2/local_execution_output.txt
```

## Step 6: Run Full Dataset YARN Check

```bash
PYSPARK_PYTHON=/usr/bin/python3.10 \
PYSPARK_DRIVER_PYTHON=/usr/bin/python3.10 \
spark-submit \
  --master yarn \
  --deploy-mode client \
  --driver-memory 512m \
  --executor-memory 1g \
  --executor-cores 1 \
  --num-executors 1 \
  --conf spark.driver.maxResultSize=128m \
  full_dataset_phaseA_check.py | tee output/milestone2/full_dataset_phaseA_yarn_output.txt
```

---

# 24. Final Milestone 2 Readiness

After the final fixes, the repository includes:

- Spark analytics for Milestone 2
- M1 vs M2 comparison
- standalone Spark ML pipeline
- correct required feature vector
- three trained models
- model comparison metrics
- confusion matrices
- feature importance interpretation
- local execution evidence
- YARN client-mode ML evidence
- full dataset YARN evidence
- saved output logs for grading verification

Therefore, the Milestone 2 implementation is complete and ready for final submission.

---

# 25. Conclusion

Milestone 2 successfully demonstrates distributed data analytics and machine learning using Apache Spark MLlib on the Chicago Crimes dataset.

The project completed the following:

- Loaded data from HDFS using Spark.
- Verified local execution using `local[*]`.
- Verified cluster execution using YARN.
- Processed the full HDFS dataset using Spark on YARN.
- Performed Spark DataFrame analytics.
- Cleaned and prepared data for ML.
- Created the required feature vector using `District`, `crime_index`, `Hour`, and `domestic_index`.
- Trained Logistic Regression, Random Forest, and Gradient-Boosted Trees models.
- Evaluated models using accuracy, F1 score, precision, recall, AUC, and confusion matrices.
- Reported feature importance and interpretation.
- Saved full Spark-submit logs in `output/milestone2/`.

The best model based on AUC-ROC in the final standalone ML run was:

```text
Gradient-Boosted Trees
```

with:

```text
AUC = 0.7423
```

This milestone shows the complete big data machine learning workflow:

```text
HDFS → Spark DataFrames → Feature Engineering → Spark MLlib Models → Evaluation → Interpretation
```