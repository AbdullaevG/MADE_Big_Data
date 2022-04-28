#! /usr/bin/python
import sys

for row in sys.stdin:
    print(row.replace('\n', ''))

