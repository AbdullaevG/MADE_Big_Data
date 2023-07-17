#!/usr/bin/env python3
import sys

current_tag = None
current_year = None
tag_count = 0

for line in sys.stdin:
    temp_year, temp_tag, counts = line.split("\t", 3)
    counts = int(counts)
    if current_tag and temp_tag == current_tag and temp_year == current_year:
        tag_count += counts
    else:
        if current_tag:
            print(current_year, current_tag, tag_count, sep="\t")
        current_year = temp_year
        current_tag = temp_tag
        tag_count = counts

if current_tag:
    print(current_year, current_tag, tag_count, sep="\t")
