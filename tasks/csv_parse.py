#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Brief Summary
Attributes:

Google Python Style Guide:
    http://google-styleguide.googlecode.com/svn/trunk/pyguide.html
"""
__copyright__ = "Zhaoyu Luo"


import copy
import datetime


def get_json(filepath):
    headers = [
        "filename",
        "taskid",
        "tasktype",
        "type",
        "starttime",
        "finishtime",
    ]
    with open("task_types.csv", "w") as out:
        output_line = ",".join(headers)
        out.write(output_line + "\n")
        with open(filepath) as f:
            m = {}
            for h in headers:
                m[h] = ""
            for line in f:
                k, v = line.split(":")
                k = k.strip()
                k = k.strip('"')
                v = v.strip()
                v = v.strip(",")
                v = v.strip('"')
                if v.isdigit():
                    v = datetime.datetime.fromtimestamp(float(v) / 1000).strftime('%Y-%m-%d %H:%M:%S')
                if k.startswith("jhist"):
                    m["filename"] = k
                    output_line = ",".join([m[i] for i in headers])
                    out.write(output_line + "\n")
                    m = {}
                    for h in headers:
                        m[h] = ""
                else:
                    m[k.lower()] = v
            output_line = ",".join([m[i] for i in headers])
            out.write(output_line + "\n")



def main():
    """Main function only in command line"""
    from sys import argv
    get_json(argv[1])
    


if __name__ == '__main__':
    main()
