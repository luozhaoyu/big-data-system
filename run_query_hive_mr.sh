#!/bin/sh
if [ -z "$1" ]
then
    printf "Please input a number!\n"
    exit 1
else
    cmd="(time hive --hiveconf hive.execution.engine=mr -f ~/workload/hive-tpcds-tpch-workload/sample-queries-tpcds/query$1.sql --database tpcds_text_db_1_10) > tpcds_query$1_mr.out 2>&1"
fi


printf "You are running $cmd...\n"
printf "check by: tail -f tpcds_query$1_mr.out\n"


echo "Start: " $(date +%s)
(time hive --hiveconf hive.execution.engine=mr -f ~/workload/hive-tpcds-tpch-workload/sample-queries-tpcds/query$1.sql --database tpcds_text_db_1_10) > tpcds_query$1_mr.out 2>&1
echo "End: " $(date +%s)

