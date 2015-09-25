#!/bin/sh
cmd="hive --hiveconf hive.execution.engine=mr -f ~/workload/hive-tpcds-tpch-workload/sample-queries-tpcds/query$1.sql --database tpcds_text_db_1_10 > run_mr_$1.txt 2>&1"
printf "You are running $cmd...\n"
printf "check by: tail -f run_mr_$1.txt\n"
hive --hiveconf hive.execution.engine=tez -f ~/workload/hive-tpcds-tpch-workload/sample-queries-tpcds/query$1.sql --database tpcds_text_db_1_10 > run_mr_$1.txt 2>&1
echo "Start timestamp: " $(date +%s)
(time hive --hiveconf hive.execution.engine=mr -f sample-queries-tpcds/query21.sql --database tpcds_text_db_1_2) 2> output/tpcds_query21_mr.out
echo "End timestamp: " $(date +%s)
