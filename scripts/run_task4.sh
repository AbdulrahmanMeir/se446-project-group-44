#!/bin/bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC_DIR="$PROJECT_ROOT/src"
OUT_DIR="$PROJECT_ROOT/output"

INPUT_PATH="${1:-/data/chicago_crimes.csv}"
HDFS_OUTPUT="/user/${USER}/project/m1/task4"

mkdir -p "$OUT_DIR"

echo "Running Task 4: Crime Trends by Year"
echo "Input: $INPUT_PATH"
echo "HDFS Output: $HDFS_OUTPUT"

hdfs dfs -rm -r -f "$HDFS_OUTPUT" >/dev/null 2>&1 || true

mapred streaming \
  -files "$SRC_DIR/mapper_task4.py","$SRC_DIR/reducer_sum.py" \
  -mapper "python3 mapper_task4.py" \
  -reducer "python3 reducer_sum.py" \
  -input "$INPUT_PATH" \
  -output "$HDFS_OUTPUT"

hdfs dfs -cat "$HDFS_OUTPUT/part-00000" | sort -k1,1n > "$OUT_DIR/task4.txt"

echo
echo "First 5 lines:"
head -5 "$OUT_DIR/task4.txt"
echo
echo "Full sorted output saved to: $OUT_DIR/task4.txt"