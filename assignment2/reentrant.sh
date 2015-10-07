#!/bin/sh
host=$1
ssh $host mkdir -p $HOME/storage/data/spark/rdds_shuffle
# robert's bug
ssh $host mkdir -p $HOME/storage/data/spark/rdds_map
ssh $host mkdir -p $HOME/storage/logs
ssh $host mkdir -p $HOME/logs/spark
ssh $host mkdir -p $HOME/logs/operation_logs
ssh $host mkdir -p $HOME/storage/date/spark/woker
ssh $host mkdir -p $HOME/big-data-system/assignment2/output
scp run_spark.sh $host:$HOME/big-data-system/assignment2/
ssh $host "ln -sf $HOME/big-data-system/assignment2/run_spark.sh $HOME/run.sh"
