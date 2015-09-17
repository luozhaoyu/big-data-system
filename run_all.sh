#!/bin/sh
ssh ubuntu@group-2-vm1 "killall dstat"
ssh ubuntu@group-2-vm1 "rm -f ~/dstat.csv"
ssh ubuntu@group-2-vm2 "killall dstat"
ssh ubuntu@group-2-vm2 "rm -f ~/dstat.csv"
ssh ubuntu@group-2-vm3 "killall dstat"
ssh ubuntu@group-2-vm3 "rm -f ~/dstat.csv"
ssh ubuntu@group-2-vm4 "killall dstat"
ssh ubuntu@group-2-vm4 "rm -f ~/dstat.csv"

ssh ubuntu@group-2-vm1 "dstat -ta --noheaders --noupdate --output ~/dstat.csv >/dev/null 2>&1 &"
ssh ubuntu@group-2-vm2 "dstat -ta --noheaders --noupdate --output ~/dstat.csv >/dev/null 2>&1 &"
ssh ubuntu@group-2-vm3 "dstat -ta --noheaders --noupdate --output ~/dstat.csv >/dev/null 2>&1 &"
ssh ubuntu@group-2-vm4 "dstat -ta --noheaders --noupdate --output ~/dstat.csv >/dev/null 2>&1 &"

printf "start queries\n"

rm -f tpcds_query12_mr.out
./run_query_hive_mr.sh 12
sleep 5
rm -f tpcds_query21_mr.out
./run_query_hive_mr.sh 21
sleep 5
rm -f tpcds_query50_mr.out
./run_query_hive_mr.sh 50
sleep 5
rm -f tpcds_query71_mr.out
./run_query_hive_mr.sh 71
sleep 5
rm -f tpcds_query85_mr.out
./run_query_hive_mr.sh 85
sleep 5
rm -f tpcds_query12_tez.out
./run_query_hive_tez.sh 12
sleep 5
rm -f tpcds_query21_tez.out
./run_query_hive_tez.sh 21
sleep 5
rm -f tpcds_query50_tez.out
./run_query_hive_tez.sh 50
sleep 5
rm -f tpcds_query71_tez.out
./run_query_hive_tez.sh 71
sleep 5
rm -f tpcds_query85_tez.out
./run_query_hive_tez.sh 85

ssh ubuntu@group-2-vm1 "killall dstat"
ssh ubuntu@group-2-vm2 "killall dstat"
ssh ubuntu@group-2-vm3 "killall dstat"
ssh ubuntu@group-2-vm4 "killall dstat"
printf "I finished!! oh yeah!!!\n"
