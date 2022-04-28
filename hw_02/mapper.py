#! /usr/bin/python

import sys
import random


num = random.randint(1, 5)
row = ""
count = 0

for line in sys.stdin:
    if count < num:
        row += line.replace("\n", "") + ","
        count +=1
    else:
        print(row)
        row = ""
        count = 0
        num = random.randint(1, 5)

