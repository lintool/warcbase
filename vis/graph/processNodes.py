#!/usr/bin/python

import re
import operator
import sys

nodes = {}

class Node:
    def __init__(self, name, count):
        self.name = name
        self.count = count

with open('raw.txt') as fp:
    for line in fp:
        temp = re.split("\t", line.rstrip())
        arr = []
        for idx, val in enumerate(temp):
            arr.append(val.replace(" ", ""))
        sourceName = arr[1]
        destName = arr[2]
        count = int(arr[3])
        if sourceName in nodes:
            nodes[sourceName].count += count
        else:
            nodes[sourceName] = Node(sourceName, count)
        if destName in nodes:
            nodes[destName].count += count
        else:
            nodes[destName] = Node(destName, count)

sys.stdout.write("name")
sys.stdout.write(",")
sys.stdout.write("count\n")

for node in sorted(nodes.values(), key=operator.attrgetter('count'), reverse=True):
    sys.stdout.write(node.name)
    sys.stdout.write(",")
    sys.stdout.write(str(node.count))
    sys.stdout.write("\n")
