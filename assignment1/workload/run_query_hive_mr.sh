#!/bin/sh

mkdir output
echo "Start: " $(date +%s)
#(time hive --hiveconf hive.execution.engine=mr -f sample-queries-tpcds/query$1.sql --database tpcds_text_db_1_2) 2> output/tpcds_query$1_mr.out
(time hive --hiveconf hive.execution.engine=mr -f sample-queries-tpcds/query.sql --database tpcds_text_db_1_2) 2> output/tpcds_query_mr.out
echo "End: " $(date +%s)

