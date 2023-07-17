#!/usr/bin/env python3
import sys
years = ["2010", "2016"]

for line in sys.stdin:
    year, tag, count = line.strip().split("\t", 3)
    if year in years:
        print(year, count, tag, sep="\t")
