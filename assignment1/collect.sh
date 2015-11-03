#!/bin/sh
folder="dstat-`date +%Y-%m-%d`"
mkdir $folder
scp ubuntu@group-2-vm1:~/dstat.csv $folder/vm1.dstat.csv
scp ubuntu@group-2-vm2:~/dstat.csv $folder/vm2.dstat.csv
scp ubuntu@group-2-vm3:~/dstat.csv $folder/vm3.dstat.csv
scp ubuntu@group-2-vm4:~/dstat.csv $folder/vm4.dstat.csv

cp tpcds_query* $folder
hdfs dfs -copyToLocal /tmp/hadoop-yarn/staging/history $folder
hdfs dfs -copyToLocal /tmp/tez-history $folder
