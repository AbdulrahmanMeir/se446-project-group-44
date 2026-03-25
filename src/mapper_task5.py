#!/usr/bin/env python3
import sys
import csv

reader = csv.reader(sys.stdin)
next(reader, None)  # skip header

for row in reader:
    if len(row) > 8:
        arrest_status = row[8].strip()
        if arrest_status:
            print(f"{arrest_status}\t1")