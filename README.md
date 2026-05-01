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

This project analyzes the Chicago Crimes dataset using big data tools and distributed computing techniques. The project is divided into milestones.

- **Milestone 1:** Hadoop MapReduce analytics using Python mapper and reducer scripts.
- **Milestone 2:** Apache Spark and Spark MLlib machine learning pipeline for arrest prediction.

The main goal of the project is to demonstrate the ability to process, analyze, and model large-scale crime data using a distributed cluster environment.

---

# 2. Dataset

The dataset used in this project is the Chicago Crimes dataset stored in HDFS on the university Hadoop/Spark cluster.

## HDFS Dataset Paths

```bash
hdfs:///data/chicago_crimes.csv
hdfs:///data/chicago_crimes_sample.csv
```

For Milestone 2 execution, the sample dataset was used:

```bash
hdfs:///data/chicago_crimes_sample.csv
```

## Dataset Summary from Spark

From the Spark execution logs:

- Total rows: **10,000**
- Total columns: **22**
- Spark version: **3.5.4**
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
│       └── task5_crossvalidator_output.txt
│
├── m2_full_pipeline_attempt.py
├── m2_task1_analytics.py
├── m2_task2_logistic.py
├── m2_task3_random_forest.py
├── m2_task4_gbt.py
└── m2_task5_crossvalidator.py
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

# 6. Milestone 2 Design Decision

A full Spark ML pipeline was initially implemented in:

```text
m2_full_pipeline_attempt.py
```

However, when the full pipeline was executed as one large job, the cluster terminated the job during model training because of memory limitations.

To solve this problem, the final implementation was split into independent Spark-submit scripts:

```text
m2_task1_analytics.py
m2_task2_logistic.py
m2_task3_random_forest.py
m2_task4_gbt.py
m2_task5_crossvalidator.py
```

This design was selected because each task runs independently on YARN, avoids memory termination, and still demonstrates the full Spark ML workflow required for the milestone.

---

# 7. Milestone 2 Scripts

## Task 1 - Spark Analytics

**File:** `m2_task1_analytics.py`

This script loads the Chicago Crimes sample dataset from HDFS and performs basic Spark DataFrame analytics.

It includes:

- Dataset loading from HDFS
- Schema inspection
- Sample record display
- Crimes per year
- Top crime types
- Arrest distribution
- District-level crime counts

## Task 2 - Logistic Regression

**File:** `m2_task2_logistic.py`

This script trains a Logistic Regression model using Spark MLlib.

It includes:

- Data cleaning
- Label creation
- StringIndexer for categorical encoding
- VectorAssembler for feature vector creation
- Logistic Regression training
- Model evaluation
- Confusion matrix
- Prediction samples

## Task 3 - Random Forest

**File:** `m2_task3_random_forest.py`

This script trains a Random Forest model.

It includes:

- Spark ML Pipeline
- Random Forest classifier
- Evaluation metrics
- Confusion matrix
- Feature importance analysis

The model was configured with a small number of trees and limited depth to fit the university cluster memory limits.

## Task 4 - Gradient-Boosted Tree

**File:** `m2_task4_gbt.py`

This script trains a Gradient-Boosted Tree model.

It includes:

- Spark ML Pipeline
- GBTClassifier
- Model evaluation
- Confusion matrix
- Feature importance analysis

The number of boosting iterations and tree depth were reduced to avoid memory issues on the cluster.

## Task 5 - CrossValidator

**File:** `m2_task5_crossvalidator.py`

This script performs hyperparameter tuning using Spark MLlib CrossValidator.

It includes:

- Random Forest estimator
- ParamGridBuilder
- CrossValidator
- F1-score based evaluation
- Best parameter reporting
- Tuned model evaluation

The parameter grid was intentionally kept small because the university cluster has limited memory.

---

# 8. Features Used for Machine Learning

The following features were selected for Milestone 2 ML training:

```text
Primary Type
District
Year
Domestic
```

After preprocessing, the final feature vector used:

```text
PrimaryTypeIndex
District
Year
DomesticIndex
```

## Feature Engineering

- `Primary Type` was converted into `PrimaryTypeIndex` using `StringIndexer`.
- `Domestic` was converted into `DomesticIndex`.
- Numeric missing values were filled.
- `VectorAssembler` was used to create the final `features` column.

---

# 9. Label Distribution

The dataset has an imbalanced label distribution:

```text
label = 0 (No Arrest): 8717
label = 1 (Arrest):    1283
```

This means most records did not result in an arrest.

Because of this imbalance, accuracy alone is not enough to evaluate the models. F1 score, precision, recall, AUC, and the confusion matrix are also important.

---

# 10. Spark Submit Commands

All Milestone 2 tasks were executed on the cluster using `spark-submit`.

The following configuration was used to force Spark to use Python 3.10 because Python 3.12 on the cluster did not have the required `numpy` dependency for Spark MLlib.

## Task 1 - Analytics

```bash
spark-submit \
  --conf spark.pyspark.python=/usr/bin/python3.10 \
  --conf spark.pyspark.driver.python=/usr/bin/python3.10 \
  --conf spark.ui.showConsoleProgress=false \
  m2_task1_analytics.py 2>&1 | tee output/milestone2/task1_analytics_output.txt
```

## Task 2 - Logistic Regression

```bash
spark-submit \
  --conf spark.pyspark.python=/usr/bin/python3.10 \
  --conf spark.pyspark.driver.python=/usr/bin/python3.10 \
  --conf spark.ui.showConsoleProgress=false \
  m2_task2_logistic.py 2>&1 | tee output/milestone2/task2_logistic_output.txt
```

## Task 3 - Random Forest

```bash
spark-submit \
  --conf spark.pyspark.python=/usr/bin/python3.10 \
  --conf spark.pyspark.driver.python=/usr/bin/python3.10 \
  --conf spark.ui.showConsoleProgress=false \
  m2_task3_random_forest.py 2>&1 | tee output/milestone2/task3_random_forest_output.txt
```

## Task 4 - Gradient-Boosted Tree

```bash
spark-submit \
  --conf spark.pyspark.python=/usr/bin/python3.10 \
  --conf spark.pyspark.driver.python=/usr/bin/python3.10 \
  --conf spark.ui.showConsoleProgress=false \
  m2_task4_gbt.py 2>&1 | tee output/milestone2/task4_gbt_output.txt
```

## Task 5 - CrossValidator

```bash
spark-submit \
  --conf spark.pyspark.python=/usr/bin/python3.10 \
  --conf spark.pyspark.driver.python=/usr/bin/python3.10 \
  --conf spark.ui.showConsoleProgress=false \
  m2_task5_crossvalidator.py 2>&1 | tee output/milestone2/task5_crossvalidator_output.txt
```

---

# 11. Output Logs

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
```

These files include the full terminal output, Spark application information, dataset loading proof, model results, and completion messages.

---

# 12. Cluster Execution Evidence

The tasks were executed on the university Spark cluster using YARN.

Example execution evidence from the logs:

```text
Spark Version: 3.5.4
Spark Master: yarn
```

Example application IDs:

```text
Task 1: application_1771402826595_0325
Task 2: application_1771402826595_0326
Task 3: application_1771402826595_0327
Task 4: application_1771402826595_0328
Task 5: application_1771402826595_0329
```

This confirms that the code was executed on the distributed Spark cluster rather than only locally.

---

# 13. Milestone 2 Results

## 13.1 Spark Analytics Results

From Task 1:

### Dataset Summary

```text
Total rows: 10000
Total columns: 22
```

### Top Crime Types

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

### Arrest Distribution

```text
Arrest = true:   1283
Arrest = false:  8717
```

### Interpretation

The analytics show that theft is the most frequent crime type in the sample dataset. The arrest distribution shows a strong class imbalance because most records do not result in an arrest.

---

## 13.2 Model Evaluation Results

| Model | Accuracy | F1 Score | Precision | Recall | AUC |
|---|---:|---:|---:|---:|---:|
| Logistic Regression | 0.8787 | 0.8220 | 0.7721 | 0.8787 | 0.6716 |
| Random Forest | 0.8787 | 0.8220 | 0.7721 | 0.8787 | 0.5913 |
| Gradient-Boosted Tree | 0.8938 | 0.8588 | 0.8915 | 0.8938 | 0.7650 |
| Tuned Random Forest with CrossValidator | 0.8995 | 0.8767 | N/A | N/A | N/A |

---

# 14. Confusion Matrix Results

## Logistic Regression

```text
label=0, prediction=0.0: 1688
label=1, prediction=0.0: 233
```

The Logistic Regression model predicted all test records as non-arrest. This is caused by the strong class imbalance in the dataset.

## Random Forest

```text
label=0, prediction=0.0: 1688
label=1, prediction=0.0: 233
```

The Random Forest model also predicted all records as non-arrest when using the reduced tree configuration required by the cluster memory limit.

## Gradient-Boosted Tree

```text
label=0, prediction=0.0: 1683
label=0, prediction=1.0: 5
label=1, prediction=0.0: 199
label=1, prediction=1.0: 34
```

The Gradient-Boosted Tree model performed better than Logistic Regression and Random Forest because it successfully predicted some arrest cases.

## Tuned Random Forest with CrossValidator

```text
label=0, prediction=0.0: 1669
label=0, prediction=1.0: 19
label=1, prediction=0.0: 174
label=1, prediction=1.0: 59
```

The tuned Random Forest improved the number of correctly predicted arrest cases compared to the untuned Random Forest.

---

# 15. Feature Importance

## Random Forest Feature Importance

```text
PrimaryTypeIndex: 0.4423
District:         0.3991
Year:             0.1411
DomesticIndex:    0.0175
```

## Gradient-Boosted Tree Feature Importance

```text
PrimaryTypeIndex: 0.9780
District:         0.0220
Year:             0.0000
DomesticIndex:    0.0000
```

## Tuned Random Forest Feature Importance

```text
PrimaryTypeIndex: 0.9655
DomesticIndex:    0.0345
District:         0.0000
Year:             0.0000
```

## Interpretation

The most important feature across the tree-based models is `PrimaryTypeIndex`, which represents the crime category. This suggests that the type of crime is the strongest predictor of whether an arrest occurs.

District also contributed in the Random Forest model, but it was less important in the Gradient-Boosted Tree and tuned Random Forest models.

---

# 16. Best Model

The best model based on F1 score was:

```text
Tuned Random Forest with CrossValidator
```

with:

```text
Accuracy: 0.8995
F1 Score: 0.8767
```

However, the Gradient-Boosted Tree model also performed strongly:

```text
Accuracy: 0.8938
F1 Score: 0.8588
AUC: 0.7650
```

The tuned Random Forest achieved the highest F1 score, while the Gradient-Boosted Tree achieved the strongest AUC among the non-tuned models.

---

# 17. CrossValidator Results

The CrossValidator tested:

```text
Parameter combinations: 4
Folds: 2
```

The best Random Forest parameters were:

```text
numTrees: 2
maxDepth: 2
```

The grid was intentionally kept small because the university cluster has limited memory.

This still demonstrates the correct use of Spark MLlib hyperparameter tuning with:

- `ParamGridBuilder`
- `CrossValidator`
- F1-score based evaluation

---

# 18. Challenges and Solutions

## Challenge 1: Python 3.12 Missing NumPy

The default PySpark environment used Python 3.12, but Spark MLlib required `numpy`, which was not available for Python 3.12 on the cluster.

### Solution

The Spark jobs were submitted with Python 3.10:

```bash
--conf spark.pyspark.python=/usr/bin/python3.10
--conf spark.pyspark.driver.python=/usr/bin/python3.10
```

Python 3.10 had the required `numpy` package installed.

## Challenge 2: Full Pipeline Memory Termination

The first full Spark ML pipeline attempted to train multiple models and CrossValidator in one script. The cluster killed the job during training due to memory limitations.

### Solution

The final implementation split the milestone into separate Spark-submit scripts. Each script runs one task independently, reducing memory pressure and allowing all required tasks to complete successfully.

## Challenge 3: Class Imbalance

The dataset is imbalanced:

```text
No Arrest: 8717
Arrest:    1283
```

This caused baseline models to predict mostly non-arrest cases.

### Solution

Multiple metrics were reported, including:

- Accuracy
- F1 Score
- Precision
- Recall
- AUC
- Confusion Matrix

This gives a more honest evaluation than accuracy alone.

---

# 19. How to Reproduce Milestone 2

## Step 1: Connect to the Cluster

```bash
ssh ameir@134.209.172.50
```

## Step 2: Enter the Repository

```bash
cd ~/se446-project-group-44
git checkout milestone-2-spark-ml
git pull
```

## Step 3: Create Output Folder

```bash
mkdir -p output/milestone2
```

## Step 4: Run Each Spark Job

Run Task 1:

```bash
spark-submit \
  --conf spark.pyspark.python=/usr/bin/python3.10 \
  --conf spark.pyspark.driver.python=/usr/bin/python3.10 \
  --conf spark.ui.showConsoleProgress=false \
  m2_task1_analytics.py 2>&1 | tee output/milestone2/task1_analytics_output.txt
```

Run Task 2:

```bash
spark-submit \
  --conf spark.pyspark.python=/usr/bin/python3.10 \
  --conf spark.pyspark.driver.python=/usr/bin/python3.10 \
  --conf spark.ui.showConsoleProgress=false \
  m2_task2_logistic.py 2>&1 | tee output/milestone2/task2_logistic_output.txt
```

Run Task 3:

```bash
spark-submit \
  --conf spark.pyspark.python=/usr/bin/python3.10 \
  --conf spark.pyspark.driver.python=/usr/bin/python3.10 \
  --conf spark.ui.showConsoleProgress=false \
  m2_task3_random_forest.py 2>&1 | tee output/milestone2/task3_random_forest_output.txt
```

Run Task 4:

```bash
spark-submit \
  --conf spark.pyspark.python=/usr/bin/python3.10 \
  --conf spark.pyspark.driver.python=/usr/bin/python3.10 \
  --conf spark.ui.showConsoleProgress=false \
  m2_task4_gbt.py 2>&1 | tee output/milestone2/task4_gbt_output.txt
```

Run Task 5:

```bash
spark-submit \
  --conf spark.pyspark.python=/usr/bin/python3.10 \
  --conf spark.pyspark.driver.python=/usr/bin/python3.10 \
  --conf spark.ui.showConsoleProgress=false \
  m2_task5_crossvalidator.py 2>&1 | tee output/milestone2/task5_crossvalidator_output.txt
```

---

# 20. Conclusion

Milestone 2 successfully demonstrates distributed data analytics and machine learning using Apache Spark MLlib on the Chicago Crimes dataset.

The project completed the following:

- Loaded data from HDFS using Spark.
- Verified cluster execution using YARN.
- Performed Spark DataFrame analytics.
- Cleaned and prepared data for ML.
- Created feature vectors using `StringIndexer` and `VectorAssembler`.
- Trained Logistic Regression, Random Forest, and Gradient-Boosted Tree models.
- Evaluated models using accuracy, F1 score, precision, recall, AUC, and confusion matrices.
- Used CrossValidator for hyperparameter tuning.
- Saved full Spark-submit logs in `output/milestone2/`.

The best model based on F1 score was the tuned Random Forest with CrossValidator, while the Gradient-Boosted Tree achieved the strongest AUC among the non-tuned models.

This milestone shows the complete big data machine learning workflow:

```text
HDFS → Spark DataFrames → Feature Engineering → Spark MLlib Models → Evaluation → Interpretation
```