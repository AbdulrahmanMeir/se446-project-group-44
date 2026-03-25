#!/usr/bin/env python3
import sys
import csv

reader = csv.reader(sys.stdin)
next(reader, None)  # skip header

for row in reader:
    if len(row) > 5:
        primary_type = row[5].strip()
        if primary_type:
            print(f"{primary_type}\t1")