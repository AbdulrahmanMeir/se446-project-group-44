# ============================================
# SE446 - Milestone 2: Spark ML Pipeline
# Group 44
# Student: Abdulrahman Meir
#
# This file is the required standalone Spark-submit entry point.
# The original full pipeline was split into separate scripts because
# the university cluster terminated the large combined job due to memory limits.
#
# Final executed scripts:
# - m2_task1_analytics.py
# - m2_task2_logistic.py
# - m2_task3_random_forest.py
# - m2_task4_gbt.py
# - m2_task5_crossvalidator.py
# ============================================

import subprocess
import sys


TASKS = [
    "m2_task2_logistic.py",
    "m2_task3_random_forest.py",
    "m2_task4_gbt.py",
]


def run_task(script_name):
    print("=" * 80)
    print(f"Running {script_name}")
    print("=" * 80)

    result = subprocess.run(
        [sys.executable, script_name],
        text=True
    )

    if result.returncode != 0:
        print(f"Task failed: {script_name}")
        sys.exit(result.returncode)

    print(f"Completed: {script_name}")


if __name__ == "__main__":
    print("=" * 80)
    print("SE446 Milestone 2 - Standalone Spark ML Script")
    print("=" * 80)
    print("This script runs the main ML model scripts sequentially.")
    print("For full output logs, see output/milestone2/.")

    for task in TASKS:
        run_task(task)

    print("=" * 80)
    print("m2_spark_ml.py completed successfully.")
    print("=" * 80)