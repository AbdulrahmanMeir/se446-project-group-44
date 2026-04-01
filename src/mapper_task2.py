#!/usr/bin/env python3
import sys
import csv

reader = csv.reader(sys.stdin)

for row in reader:
    if not row:
        continue

    if row[0].strip() == "ID":
        continue

    if len(row) <= 5:
        continue

    primary_type = row[5].strip()
    if primary_type:
        print(f"{primary_type}\t1")