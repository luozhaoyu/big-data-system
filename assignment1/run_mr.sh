#!/bin/sh
cmd="(time hive --hiveconf hive.execution.engine=mr -f ~/workload/hive-tpcds-tpch-workload/sample-queries-tpcds/query$1.sql --database tpcds_text_db_1_10) > tpcds_query$1_mr.out 2>&1"
printf "You are running $cmd...\n"
printf "check by: tail -f tpcds_query$1.txt\n"


echo "Start: " $(date +%s)
(time hive --hiveconf hive.execution.engine=mr -f ~/workload/hive-tpcds-tpch-workload/sample-queries-tpcds/query$1.sql --database tpcds_text_db_1_10) > tpcds_query$1_mr.out 2>&1
echo "End: " $(date +%s)
