#!/usr/bin/env python3
import sys
import csv
import re

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

    match = re.match(r"^\d{1,2}/\d{1,2}/(\d{4})\b", date_value)
    if not match:
        continue

    year = match.group(1)

    if 2000 <= int(year) <= 2026:
        print(f"{year}\t1")