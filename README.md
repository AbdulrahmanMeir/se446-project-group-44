# SE446 – Big Data Engineering  
## Milestone 1: Chicago Crime Analytics with MapReduce

**Repository:** `se446-project-group-44`  
**Group Number:** 44  

---

## Team Members

| Name | Student ID | GitHub Username | Assigned Task |
|---|---:|---|---|
| Abdulrahman Meir | [210450] | [AbdulrahmanMeir] | [Milestone_1] |




---

## Executive Summary

This project implements four Hadoop MapReduce analytics tasks on the Chicago Crimes dataset using Python Hadoop Streaming. The goal is to answer key law-enforcement questions by processing a large crime dataset on the Hadoop cluster. The implemented tasks are: crime type distribution, location hotspots, crime trends by year, and arrest status analysis. Each task uses a custom Python mapper and a shared reducer to aggregate counts. The jobs were first tested on the sample dataset and then executed on the full dataset `/data/chicago_crimes.csv` for final submission. The repository contains the source code, execution scripts, and output files for all required tasks.

---

## Project Structure

```text
se446-project-group-44/
├── README.md
├── src/
│   ├── mapper_task2.py
│   ├── mapper_task3.py
│   ├── mapper_task4.py
│   ├── mapper_task5.py
│   └── reducer_sum.py
├── scripts/
│   ├── run_task2.sh
│   ├── run_task3.sh
│   ├── run_task4.sh
│   └── run_task5.sh
├── output/
│   ├── task2.txt
│   ├── task3.txt
│   ├── task4.txt
│   └── task5.txt
└── test_data.csv

Environment and Execution Notes

The jobs were executed on the Hadoop cluster using Hadoop Streaming.
For the final submission, the input dataset used is:
/data/chicago_crimes.csv

Before running any task on the cluster, the Hadoop environment must be loaded:
source /etc/profile.d/hadoop.sh

Task 2 – Crime Type Distribution
Research Question

What are the most common types of crimes in Chicago?

Logic

This task reads the Primary Type field at index 5 from each CSV row and emits (crime_type, 1).
The reducer sums the values for each crime type to get the total number of occurrences.

Exact Hadoop Streaming Command

mapred streaming \
  -files /home/ameir/se446-project-group-44/src/mapper_task2.py,/home/ameir/se446-project-group-44/src/reducer_sum.py \
  -mapper "python3 mapper_task2.py" \
  -reducer "python3 reducer_sum.py" \
  -input /data/chicago_crimes.csv \
  -output /user/ameir/project/m1/task2

Top 5 Output Lines

THEFT	2054
BATTERY	1728
CRIMINAL DAMAGE	1062
MOTOR VEHICLE THEFT	948
ASSAULT	878


Interpretation

Theft appears to be the most frequent crime category, which suggests it should receive high operational attention.

Full Execution Log
[PASTE_FULL_TERMINAL_LOG_FOR_TASK2_HERE]



Task 3 – Location Hotspots
Research Question

Where do most crimes occur?

Logic

This task reads the Location Description field at index 7 from each CSV row and emits (location_description, 1).
The reducer sums the values to identify the locations with the highest crime counts.

Exact Hadoop Streaming Command

mapred streaming \
  -files /home/ameir/se446-project-group-44/src/mapper_task3.py,/home/ameir/se446-project-group-44/src/reducer_sum.py \
  -mapper "python3 mapper_task3.py" \
  -reducer "python3 reducer_sum.py" \
  -input /data/chicago_crimes.csv \
  -output /user/ameir/project/m1/task3

  Top 5 Output Lines

STREET	2737
APARTMENT	1909
RESIDENCE	1358
SIDEWALK	536
PARKING LOT / GARAGE (NON RESIDENTIAL)	362

Interpretation

Crimes are concentrated in public and residential spaces, especially streets and apartments.

Full Execution Log
[PASTE_FULL_TERMINAL_LOG_FOR_TASK3_HERE]


Task 4 – Crime Trends by Year
Research Question

How has the total number of crimes changed over the years?

Logic

This task reads the Date field at index 2, extracts the year from the date string, and emits (year, 1).
The reducer sums the counts to produce the total number of crimes per year.

Exact Hadoop Streaming Command
mapred streaming \
  -files /home/ameir/se446-project-group-44/src/mapper_task4.py,/home/ameir/se446-project-group-44/src/reducer_sum.py \
  -mapper "python3 mapper_task4.py" \
  -reducer "python3 reducer_sum.py" \
  -input /data/chicago_crimes.csv \
  -output /user/ameir/project/m1/task4

  Top 5 Output Lines

2023	9446
2022	135
2021	82
2017	49
2024	39


Interpretation

The yearly totals show how crime volume changed over time and can be used to observe long-term trends.

Full Execution Log
[PASTE_FULL_TERMINAL_LOG_FOR_TASK3_HERE]


Task 5 – Arrest Analysis
Research Question

What percentage of crimes result in an arrest?

Logic

This task reads the Arrest field at index 8 and emits (arrest_status, 1).
The reducer sums the values for true and false to compare arrest outcomes.

Exact Hadoop Streaming Command

mapred streaming \
  -files /home/ameir/se446-project-group-44/src/mapper_task5.py,/home/ameir/se446-project-group-44/src/reducer_sum.py \
  -mapper "python3 mapper_task5.py" \
  -reducer "python3 reducer_sum.py" \
  -input /data/chicago_crimes.csv \
  -output /user/ameir/project/m1/task5

   Top 5 Output Lines

false	8717
true	1282

Interpretation

The results show that non-arrest cases are much more common than arrest cases in the dataset.

Full Execution Log
[PASTE_FULL_TERMINAL_LOG_FOR_TASK3_HERE]