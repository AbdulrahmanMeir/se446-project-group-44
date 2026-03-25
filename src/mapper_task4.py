#!/usr/bin/env python3
import sys
import csv

reader = csv.reader(sys.stdin)
next(reader, None)  # skip header

for row in reader:
    if len(row) > 2:
        date_value = row[2].strip()
        if date_value:
            parts = date_value.split('/')
            if len(parts) >= 3:
                year = parts[2].split()[0]
                if year:
                    print(f"{year}\t1")