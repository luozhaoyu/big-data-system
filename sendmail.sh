#!/bin/sh
file=$1
gzip -c $1 | uuencode $1.gz | mail -s "$1" codingmiao@gmail.com
