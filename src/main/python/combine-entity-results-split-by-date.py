#!/usr/bin/env python

import os, sys
import fnmatch
import gzip
import re

part_pattern = 'part-m-*.gz'

def combine(input_dir, output_dir):
    part_file_names = fnmatch.filter(os.listdir(input_dir), part_pattern)
    date = ''
    dates = set()
    try:
        for p in part_file_names:
            part_f = gzip.open(os.path.join(input_dir, p), mode='r')
            lines = part_f.readlines()
            part_f.close()
            for line in lines:
                prev_date = date
                date = get_line_date(line)
                if date != prev_date:   # new/different date found
                    try:
                        # Or should we instead maintain a growing list
                        # of file handles and close them all at the end?
                        pers_f.close()  # close files for prev date
                        org_f.close()
                        loc_f.close()
                    except NameError:   # first time, undeclared vars
                        pass
                    except:
                        print "Unexpected error closing file"
                    if date in dates:   # date already seen, append to files
                        writemode = 'a'
                    else:
                        writemode = 'w' # new date, begin new file
                        dates.add(date)
                    pers_f = open(os.path.join(output_dir, date + '_pers.txt'), mode=writemode)
                    org_f = open(os.path.join(output_dir, date + '_org.txt'), mode=writemode)
                    loc_f = open(os.path.join(output_dir, date + '_loc.txt'), mode=writemode)
                # end if
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
            combine(input_dir, output_dir)
        else:
            print "Invalid directory name(s)"
    else:
        print "Usage: %s <dir containing %s files> [output dir]" % (sys.argv[0], part_pattern)
