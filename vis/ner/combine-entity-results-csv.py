#!/usr/bin/env python

import os, sys
import fnmatch
import gzip
import re
import csv

part_pattern = 'part-*.gz'

def combine(input_dir, output_dir):
    part_file_names = fnmatch.filter(os.listdir(input_dir), part_pattern)
    data = {}
    try:
        out_f = open(os.path.join(output_dir, 'out.csv'), mode='w')
        csvwriter = csv.writer(out_f)
        for p in part_file_names:
            part_f = gzip.open(os.path.join(input_dir, p), mode='r')
            lines = part_f.readlines()
            part_f.close()
            for line in lines:
                m = re.search('^(.*?)\t(.*?)\t{PERSON=\[(.*)\], ORGANIZATION=' \
                              '\[(.*)\], LOCATION=\[(.*)\]}', line)
                if m:
                    if m.group(3):
                        pers = m.group(3).split(", ")
                        for item in pers:
                            data[("pers", m.group(1), m.group(2), item)] = data.get(("pers", m.group(1), m.group(2), item), 0) + 1
                    if m.group(4):
                        org = m.group(4).split(", ")
                        for item in org:
                            data[("org", m.group(1), m.group(2), item)] = data.get(("org", m.group(1), m.group(2), item), 0) + 1
                    if m.group(5):
                        loc = m.group(5).split(", ")
                        for item in loc:
                            data[("loc", m.group(1), m.group(2), item)] = data.get(("loc", m.group(1), m.group(2), item), 0) + 1
                else:
                    print "Error: Expected matching entity string"
        for (ner, date, src, entity), freq in data.iteritems():
            csvwriter.writerow([ner, date, src, entity, freq])
        out_f.close()
    except IOError as e:
        print "Operation failed: %s" % e.strerror
    return

if __name__ == '__main__':
    if len(sys.argv) >= 2:
        input_dir = sys.argv[1]
        if len(sys.argv) == 3:
            output_dir = sys.argv[2]
        else:
            output_dir = input_dir
        if os.path.isdir(input_dir) and os.path.isdir(output_dir):
            combine(input_dir, output_dir)
        else:
            print "Invalid directory name(s)"
    else:
        print "Usage: %s <dir containing %s files> [output dir]" % (sys.argv[0], part_pattern)
