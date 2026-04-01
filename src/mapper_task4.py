#!/usr/bin/env python3
import sys
import csv

reader = csv.reader(sys.stdin)

for row in reader:
    if not row:
        continue

    if row[0].strip() == "ID":
        continue

    if len(row) <= 2:
        continue

    date_value = row[2].strip()
    if not date_value:
        continue

    try:
        date_part = date_value.split()[0]
        date_parts = date_part.split("/")
        if len(date_parts) == 3:
            year = date_parts[2].strip()
            if len(year) == 4 and year.isdigit():
                print(f"{year}\t1")
    except Exception:
        continue