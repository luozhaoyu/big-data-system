#!/usr/env/python
# -*- coding: utf-8 -*-
import os, sys, argparse
sys.path.append(os.path.join(os.environ['SPARK_HOME'], "python"))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], "python/lib/py4j-0.8.2.1-src.zip"))
from shell import execute, execute_in_background

import pyspark
from pyspark import SparkContext, SparkConf


def main():
    parser = argparse.ArgumentParser(description="for question 3")
    parser.add_argument("-q", "--query", help="query for question3", default="")
    args = parser.parse_args()

    cmd = "spark-submit $HOME/big-data-system/assignment2/query_q3.py"
    # run with SparkAPI
    res = execute(cmd, verbose=True)
    print res

if __name__ == '__main__':
    main()
