#!/usr/bin/env python3
import sys
import csv

reader = csv.reader(sys.stdin)
next(reader, None)  # skip header

for row in reader:
    if len(row) > 7:
        location_description = row[7].strip()
        if location_description:
            print(f"{location_description}\t1")