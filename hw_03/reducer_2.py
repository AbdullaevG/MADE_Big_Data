#!/usr/bin/env python3
import sys

count_2010 = 0
count_2016 = 0

for line in sys.stdin:
    year, counts, tag = line.strip().split("\t", 3)
    if year == "2010" and count_2010 < 10:
        count_2010 += 1
        print(year, tag, counts, sep="\t")
    if year == "2016" and count_2016 < 10:
        count_2016 += 1
        print(year, tag, counts, sep="\t")
