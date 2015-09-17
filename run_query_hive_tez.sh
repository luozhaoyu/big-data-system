#!/bin/sh
containerJvm="-Xmx4600m"
containerSize=4800

if [ -z "$1" ]
then
    printf "Please input a number!\n"
    exit 1
else
    cmd="(time hive --hiveconf hive.execution.engine=tez --hiveconf hive.tez.container.size=$containerSize --hiveconf hive.tez.java.opts=$containerJvm -f ~/workload/hive-tpcds-tpch-workload/sample-queries-tpcds/query$1.sql --database tpcds_text_db_1_10) 2> tpcds_query$1_tez.out"
fi


printf "You are running $cmd...\n"
printf "check by: tail -f tpcds_query$1_tez.out\n"


echo "Start: " $(date +%s)
(time hive --hiveconf hive.execution.engine=tez --hiveconf hive.tez.container.size=$containerSize --hiveconf hive.tez.java.opts=$containerJvm -f ~/workload/hive-tpcds-tpch-workload/sample-queries-tpcds/query$1.sql --database tpcds_text_db_1_10) 2> tpcds_query$1_tez.out
echo "End: " $(date +%s)

