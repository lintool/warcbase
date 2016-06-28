#!/usr/bin/env python

import os, sys
import fnmatch
import gzip
import re

part_pattern = 'part-*.gz'

def combine(input_dir, output_dir):
    part_file_names = fnmatch.filter(os.listdir(input_dir), part_pattern)
    try:
        pers_f = open(os.path.join(output_dir, 'pers.txt'), mode='w')
        org_f = open(os.path.join(output_dir, 'org.txt'), mode='w')
        loc_f = open(os.path.join(output_dir, 'loc.txt'), mode='w')
        for p in part_file_names:
            part_f = gzip.open(os.path.join(input_dir, p), mode='r')
            lines = part_f.readlines()
            part_f.close()
            for line in lines:
                m = re.search('{PERSON=\[(.*)\], ORGANIZATION=' \
                              '\[(.*)\], LOCATION=\[(.*)\]}', line)
                if m:
                    for (f, i) in [(pers_f, 1), (org_f, 2), (loc_f, 3)]:
                        if m.group(i):
                            f.write(m.group(i).replace(', ', '\n') + '\n')
                else:
                    print "Error: Expected matching entity string"
        pers_f.close()
        org_f.close()
        loc_f.close()
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
