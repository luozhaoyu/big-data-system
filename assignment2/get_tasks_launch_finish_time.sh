#!/bin/sh
grep SparkListenerTaskEnd $1 | grep -ioP "Launch Time.*?,|Finish Time.*?,"
