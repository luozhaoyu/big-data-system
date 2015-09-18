#!/bin/sh
output=tpcds_query.tez.$1.out
containerJvm="-Xmx4600m"
containerSize=4800

if [ -z "$1" ]
then
    printf "Please input a number!\n"
    exit 1
else
    cmd="(time hive --hiveconf hive.execution.engine=tez --hiveconf hive.tez.container.size=$containerSize --hiveconf hive.tez.java.opts=$containerJvm -f ~/workload/hive-tpcds-tpch-workload/sample-queries-tpcds/query$1.sql --database tpcds_text_db_1_50) >> $output 2>&1"
fi


printf "You are running:\n$cmd...\n"
printf "check by:\ntail -f $output\n"

ssh ubuntu@group-2-vm1 "sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'"
ssh ubuntu@group-2-vm2 "sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'"
ssh ubuntu@group-2-vm3 "sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'"
ssh ubuntu@group-2-vm4 "sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'"


printf "hive start:" > $output
date +"%Y-%m-%dT%H:%M:%S" >> $output
(time hive --hiveconf hive.execution.engine=tez --hiveconf hive.tez.container.size=$containerSize --hiveconf hive.tez.java.opts=$containerJvm -f ~/workload/hive-tpcds-tpch-workload/sample-queries-tpcds/query$1.sql --database tpcds_text_db_1_50) >> $output 2>&1

if [ "$?" = "0" ]; then
    printf "job: tez_$1 succeed!\n"
else
    printf "job: tez_$1 failed!\n"
fi

printf "hive ends:" >> $output
date +"%Y-%m-%dT%H:%M:%S" >> $output
