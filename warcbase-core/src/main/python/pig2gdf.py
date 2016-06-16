#!/usr/bin/env python
# Converts pig output of format Time interval, Source, Target, Weight
# to GDF file
# -jrwiebe

import csv, sys

node = set()
edge = []

with open(sys.argv[1], 'r') if len(sys.argv) > 1 else sys.stdin as f:
	reader = csv.reader(f, delimiter='\t')
	for row in reader:
		node.add(row[1])
		node.add(row[2])
		edge.append([row[1], row[2], row[3], row[0]])	
print "nodedef> name VARCHAR"
for n in node:
	print n
print "edgedef> source VARCHAR, target VARCHAR, weight DOUBLE, timeint VARCHAR"
for e in edge:
	print "%s,%s,%s,%s" % (e[0], e[1], e[2], e[3])

