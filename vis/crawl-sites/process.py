#!/usr/bin/python

import re
import operator
import sys

top_n = 20

data = {}
counts = {}
with open('raw.txt') as fp:
  for line in fp:
    arr = re.split("\s+", line.rstrip())
    date = arr[0]
    domain = arr[1]
    count = int(arr[2])
    #print date, domain, count
    if date in data:
      data[date][domain] = count
    else:
      entry = { domain: count }
      data[date] = entry
    if arr[1] in counts:
      counts[arr[1]] += int(arr[2])
    else:
      counts[arr[1]] = int(arr[2])

sorted_counts = list(reversed(sorted(counts.items(), key=operator.itemgetter(1))))

urls = []
for i in range(0, top_n):
  #print sorted_counts[i]
  urls.append(sorted_counts[i][0])

sys.stdout.write("State")
for url in urls:
  sys.stdout.write(",")
  sys.stdout.write(url)
sys.stdout.write(",other\n")

for date in sorted(data.keys()):
  d = re.sub(r'\d\d(\d\d)', r'\1/', date)
  print d,
  for url in urls:
    #print url,
    if url in data[date]:
      sys.stdout.write(",")
      print data[date][url],
    else:
      sys.stdout.write(",0")
  other_cnt = 0
  for url in data[date]:
    if url not in urls:
      #print url, data[date][url]
      other_cnt += data[date][url]
  sys.stdout.write(",")
  sys.stdout.write(str(other_cnt))
  sys.stdout.write("\n")

