#!/usr/bin/env python3
import sys
import re
from xml.etree import ElementTree

remove_punkt = re.compile(r"[<> ]+")

for line in sys.stdin:
    
    line = line.strip()

    if line.startswith("<row"):

        line = ElementTree.fromstring(line)
        year = line.get("CreationDate")[:4]
        tags = row.get("Tags")
        
        if tags:
            tags = remove_punkt.sub(" ", tags).strip()
            for tag in tags.split():
                print(year, tag, 1, sep="\t")
