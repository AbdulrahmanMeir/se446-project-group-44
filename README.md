# SE446 Milestone 1 – MapReduce

## Student Information
- Name: Abdulrahman Meir
- Group: 44
- Repository: se446-project-group-44

---

## Executive Summary
This milestone implements a Hadoop MapReduce pipeline on the Chicago Crimes dataset using Python streaming jobs. The tasks completed are counting crimes by primary type, counting crimes by location description, counting crimes by year, and counting crimes by arrest status. The mapper scripts were developed and tested locally in VS Code using a small sample file, then uploaded to the Hadoop cluster and executed on the provided sample dataset. The reducer was reused across all tasks to aggregate counts for identical keys.

---

## Project Structure
```text
se446_milestone1/
├── output/
│   ├── task2.txt
│   ├── task3.txt
│   ├── task4.txt
│   └── task5.txt
├── scripts/
│   ├── run_task2.sh
│   ├── run_task3.sh
│   ├── run_task4.sh
│   └── run_task5.sh
├── src/
│   ├── mapper_task2.py
│   ├── mapper_task3.py
│   ├── mapper_task4.py
│   ├── mapper_task5.py
│   └── reducer_sum.py
├── test_data.csv
└── README.md