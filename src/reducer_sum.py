#!/usr/bin/env python3
import sys

current_key = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t", 1)
    if len(parts) != 2:
        continue

    key, value = parts

    try:
        count = int(value)
    except ValueError:
        continue

    if current_key == key:
        current_count += count
    else:
        if current_key is not None:
            print(f"{current_key}\t{current_count}")
        current_key = key
        current_count = count

if current_key is not None:
    print(f"{current_key}\t{current_count}")