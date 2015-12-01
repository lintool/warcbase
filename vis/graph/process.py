#!/usr/bin/python

import re
import operator
import sys

data = set()

class Link:
    def __init__(self, date, source, target, count): 
            self.date=date
            self.source=source
            self.target=target
            self.count=count

with open('raw.txt') as fp:

  for line in fp:
    temp = re.split("\t", line.rstrip())
    arr = []
    for idx, val in enumerate(temp):
      arr.append(val.replace(" ", ""))
    link = Link(arr[0][:4], arr[1], arr[2], int(arr[3]))
    data.add(link)

sys.stdout.write("date")
sys.stdout.write(",")
sys.stdout.write("source")
sys.stdout.write(",")
sys.stdout.write("target")
sys.stdout.write(",")
sys.stdout.write("count\n")

for link in data:
  sys.stdout.write(str(link.date))
  sys.stdout.write(",")
  sys.stdout.write(link.source)
  sys.stdout.write(",")
  sys.stdout.write(link.target)
  sys.stdout.write(",")
  sys.stdout.write(str(link.count))
  sys.stdout.write("\n")

