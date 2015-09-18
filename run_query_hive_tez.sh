#!/bin/sh
containerJvm="-Xmx4600m"
containerSize=4800

if [ -z "$1" ]
then
    printf "Please input a number!\n"
    exit 1
else
    cmd="(time hive --hiveconf hive.execution.engine=tez --hiveconf hive.tez.container.size=$containerSize --hiveconf hive.tez.java.opts=$containerJvm -f ~/workload/hive-tpcds-tpch-workload/sample-queries-tpcds/query$1.sql --database tpcds_text_db_1_50) 2> tpcds_query$1_tez.out"
fi


printf "You are running $cmd...\n"
printf "check by: tail -f tpcds_query$1_tez.out\n"

ssh ubuntu@group-2-vm1 "sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'"
ssh ubuntu@group-2-vm2 "sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'"
ssh ubuntu@group-2-vm3 "sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'"
ssh ubuntu@group-2-vm4 "sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'"


printf "hive start:" > tpcds_query$1_mr.out
date +"%Y-%m-%dT%H:%M:%S" >> tpcds_query$1_mr.out
(time hive --hiveconf hive.execution.engine=tez --hiveconf hive.tez.container.size=$containerSize --hiveconf hive.tez.java.opts=$containerJvm -f ~/workload/hive-tpcds-tpch-workload/sample-queries-tpcds/query$1.sql --database tpcds_text_db_1_50) >> tpcds_query$1_tez.out 2>&1

if [ "$?" = "0" ]; then
    printf "job: tez_$1 succeed!\n"
else
    printf "job: tez_$1 failed!\n"
fi

printf "hive ends:" >> tpcds_query$1_mr.out
date +"%Y-%m-%dT%H:%M:%S" >> tpcds_query$1_mr.out
