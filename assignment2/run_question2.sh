#!/bin/sh
spark-submit --verbose --driver-class-path=/usr/lib/jvm/java-1.7.0-openjdk-amd64/lib/* question2_pyspark.py -q all > question2.output 2>&1
cat question2.txt
echo "Detail information:\nvim question2.output\n"
