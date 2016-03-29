#!/usr/bin/python
from collections import OrderedDict
import json
import operator
import re
import sys

links = []
nodes = OrderedDict()

with open('data/raw.txt') as fp:
    for line in fp:
        values = re.split("\t", line.rstrip())
        arr = [v.replace(" ", "") for v in values]

        date = arr[0][:4]
        source = arr[1]
        target = arr[2]
        count = int(arr[3])

        # create link
        link = {
            "date": date,
            "source": source,
            "target": target,
            "count": count
        }
        links.append(link)

        # create node (combining counts from sources and targets)
        if source in nodes:
            nodes[source] += count
        else:
            nodes[source] = count

        if target in nodes:
            nodes[target] += count
        else:
            nodes[target] = count

data = {
    "links": links,
    "nodes": [{"name": k, "count": nodes[k]} for k in nodes.keys()]
}
with open('data/graph.json', 'w') as outfile:
    json.dump(data, outfile, indent=2)
