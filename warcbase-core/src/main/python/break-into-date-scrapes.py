#!/usr/bin/env python
# Split results of text analysis into separate files based on crawl date
# (YYYYMM.txt). Perfomance is good -- slight speedup might be achieved by
# reading more part files (e.g., >1 GB) into memory at once.

import os, sys
import fnmatch

part_pattern = 'part-m-*'

def split_files_by_date(input_dir, output_dir):
    part_file_names = fnmatch.filter(os.listdir(input_dir), part_pattern)
    for p in part_file_names:
        with open(os.path.join(input_dir, p), mode='r') as f:
            lines = f.readlines()   # Read entire part file into memory.
                                    # Will this ever be too big?
		                            # Check and set a buffer if yes.
        split_lines_by_date(output_dir, lines)
    return

def split_lines_by_date(output_dir, lines):
    writers = {}
    for l in lines:
        date = get_line_date(l)
        if date not in writers:
            # Theoretically we could open too many files here
            writers[date] = open(os.path.join(output_dir, date+'.txt'), mode='a')
        writers[date].write(l)
    return

def get_line_date(line):
    return line[0:6]

if __name__ == '__main__':
    if len(sys.argv) >= 2:
        input_dir = sys.argv[1]
        if len(sys.argv) == 3:
            output_dir = sys.argv[2]
        else:
            output_dir = input_dir
        if os.path.isdir(input_dir) and os.path.isdir(output_dir):
            print "Processing files in "  + os.path.abspath(input_dir)
            split_files_by_date(input_dir, output_dir)
        else:
            print "Invalid directory name(s)"
    else:
        print "Usage: %s <dir containing %s files> [output dir]" % (sys.argv[0], part_pattern)
