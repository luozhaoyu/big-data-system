#!/bin/sh
if [ -z $1 ]
then
    printf "please input a query number!\n"
    exit 1
else
    cmd="(time /home/ubuntu/software/spark-1.5.0-bin-hadoop2.6/bin/beeline -u jdbc:hive2://group-2-vm1:10000/tpcds_text_db_1_50 -n ubuntu -f $HOME/workload/hive-tpcds-tpch-workload/sample-queries-tpcds/query$1.sql) 2> $HOME/big-data-system/assignment2/output/tpcds_query$1_spark.out"
    echo $cmd
fi


echo "Start: " $(date +%s)

#(time /home/ubuntu/software/spark-1.5.0-bin-hadoop2.6/bin/beeline -u jdbc:hive2://group-2-vm1:10000/tpcds_text_db_1_50 -n `whoami` -p ignored -f $HOME/workload/hive-tpcds-tpch-workload/sample-queries-tpcds/query12.sql) 2> $HOME/big-data-system/assignment2/output/tpcds_query12_spark.out
eval $cmd


echo "End: " $(date +%s)
