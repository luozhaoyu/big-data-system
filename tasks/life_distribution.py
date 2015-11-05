#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Brief Summary
Attributes:

Google Python Style Guide:
    http://google-styleguide.googlecode.com/svn/trunk/pyguide.html
"""
__copyright__ = "Zhaoyu Luo"


import datetime
import re
import time


def time_elapse_to_int(t):
    splits = t.split(":")
    h = int(splits[0])
    m = int(splits[1])
    s = int(splits[2])
    total = s + m * 60 + h * 60 * 60
    return total


def get_distribution(filepath):
    with open(filepath + ".distribution.txt", "w") as out:
        l = [0] * 24 * 60 * 60
        with open(filepath) as f:
            last = None
            begin = None
            for line in f:
                try:
                    splits = re.split(r"\s+", line)
                    start = splits[0]
                    end = splits[1]
                    s = time_elapse_to_int(start)
                    e = time_elapse_to_int(end)
                    if not begin:
                        begin = s
                    last = e
                    for i in range(s, e):
                        l[i] += 1
                except:
                    continue
        for i, v in enumerate(l[begin:last]):
            line = "\t".join([str(i + begin), str(v)])
            out.write(line + '\n')


def get_distribution(filepath):
    with open(filepath + ".elapse.txt", "w") as out:
        l = [0] * 24 * 60 * 60
        with open(filepath) as f:
            last = None
            begin = None
            start = None
            end = None
            content = f.read()
            for m in re.finditer(r"\d+:\d+:\d+", content):
                try:
                    if not start:
                        start = m.group(0)
                    elif not end:
                        end = m.group(0)
                        s = time_elapse_to_int(start)
                        e = time_elapse_to_int(end)
                        if not begin:
                            begin = s
                        last = e
                        for i in range(s, e):
                            l[i] += 1
                        start = None
                        end = None
                except Exception as e:
                    raise e
        for i, v in enumerate(l[begin:last]):
            line = "\t".join([str(i + begin), str(v)])
            out.write(line + '\n')


def main():
    """Main function only in command line"""
    from sys import argv
    get_distribution(argv[1])


if __name__ == '__main__':
    main()
