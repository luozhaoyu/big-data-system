#!/bin/sh
HIVE_WORKLOAD=$HOME/workload/hive-tpcds-tpch-workload
echo 'generating "1" TPC-DS databases, which holds 10 GB of data and tables stored in HDFS.'
cd $HIVE_WORKLOAD
$HIVE_WORKLOAD/generate_data.sh 1 50

sleep 10
echo 'running run_query_hive_mr.sh'
$HIVE_WORKLOAD/run_query_hive_mr.sh
#(hive --hiveconf hive.execution.engine=mr -f sample-queries-tpcds/query.sql --database tpcds_db_1) 2> output/query_mr.out 

sleep 10
echo 'running run_query_hive_tez.sh'
$HIVE_WORKLOAD/run_query_hive_tez.sh
#(hive --hiveconf hive.cbo.enable=true --hiveconf hive.execution.engine=tez --hiveconf hive.tez.container.size=$containerSize --hiveconf hive.tez.java.opts=$containerJvm -f sample-queries-tpcds/query.sql --database tpcds_db_1) 2> output/query_tez.out
