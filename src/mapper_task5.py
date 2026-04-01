#!/usr/bin/env python3
import sys
import csv

reader = csv.reader(sys.stdin)

for row in reader:
    if not row:
        continue

    if row[0].strip() == "ID":
        continue

    if len(row) <= 8:
        continue

    arrest_status = row[8].strip().lower()
    if arrest_status in {"true", "false"}:
        print(f"{arrest_status}\t1")