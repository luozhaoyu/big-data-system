#!/usr/env/python
# -*- coding: utf-8 -*-
import datetime
import re
import subprocess
import argparse
import time

from shell import execute, execute_in_background
from threading import Timer


def rm_local_dirs():
    hosts = ["group-2-vm1", "group-2-vm2", "group-2-vm3", "group-2-vm4"]
    for host in hosts:
        execute("ssh ubuntu@%s rm -rf $SPARK_LOCAL_DIRS/*" % host, verbose=True)


def rm_eventlog_dir():
    return execute("rm -rf $HOME/storage/logs/*", verbose=True)


def collect_eventlogs(query_name):
    execute("mkdir -p output/%s" % query_name, verbose=True)
    return execute("cp -rf $HOME/storage/logs/* output/%s" % query_name, verbose=True)


def sync_caches():
    hosts = ["group-2-vm1", "group-2-vm2", "group-2-vm3", "group-2-vm4"]
    for host in hosts:
        execute("ssh ubuntu@%s \"sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'\"" % host, verbose=True)

def restart_spark():
    stop_cmd = "/home/ubuntu/software/spark-1.5.0-bin-hadoop2.6/sbin/stop-all.sh"
    start_cmd = "/home/ubuntu/software/spark-1.5.0-bin-hadoop2.6/sbin/start-all.sh"
    execute(stop_cmd, verbose=True)
    execute(start_cmd, verbose=True)

def start_thrift(custom_conf=""):
    log_file = "thriftserver.out.%s" % (datetime.datetime.isoformat(datetime.datetime.now()))
    cmd = "/usr/lib/jvm/java-1.7.0-openjdk-amd64/bin/java -cp /home/ubuntu/conf/:/home/ubuntu/software/spark-1.5.0-bin-hadoop2.6/lib/spark-assembly-1.5.0-hadoop2.6.0.jar:/home/ubuntu/software/spark-1.5.0-bin-hadoop2.6/lib/datanucleus-rdbms-3.2.9.jar:/home/ubuntu/software/spark-1.5.0-bin-hadoop2.6/lib/datanucleus-api-jdo-3.2.6.jar:/home/ubuntu/software/spark-1.5.0-bin-hadoop2.6/lib/*:/home/ubuntu/conf/:/home/ubuntu/conf/ -Xms1g -Xmx1g -XX:MaxPermSize=256m org.apache.spark.deploy.SparkSubmit --master spark://10.0.1.27:7077 --conf spark.executor.cores=4 --conf spark.executor.memory=21000m --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/home/ubuntu/storage/logs --conf spark.driver.memory=1g --conf spark.task.cpus=1 %s --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 spark-internal" % (custom_conf)
    execute_in_background(cmd, log_file)

    # wait until it gets up and listen at 100000
    while True:
        try:
            res = execute("ss -tlnp |grep 10000")
        except subprocess.CalledProcessError as e:
            if e.returncode == 1: # have not get up
                time.sleep(1)
        else:
            print res
            break
    return True


def restart_thrift(custom_conf=""):
    while True:
        kill_thrift = "ps -ef |grep hive.thriftserver|grep -v grep | awk '{print $2}' | xargs kill"
        execute(kill_thrift, ignored_returns=[123], verbose=True)
        try:
            res = execute("ss -tlnp |grep 10000")
        except subprocess.CalledProcessError as e:
            if e.returncode == 1: # have not get up
                break
        else:
            time.sleep(1)
    return start_thrift(custom_conf)


def get_disk_net_read_write():
    cmd = "grep vda1 /proc/diskstats"
    result = re.split(r"\s+", execute(cmd).strip())
    disk_read = int(result[5])
    disk_write = int(result[9])
    cmd = "grep eth0 /proc/net/dev"
    result = re.split(r"\s+", execute(cmd).strip())
    net_read = int(result[1])
    net_write = int(result[9])
    return {
        "disk_read": disk_read,
        "disk_write": disk_write,
        "net_read": net_read,
        "net_write": net_write,
    }


def get_io_bandwidth():
    e = None
    s = None
    for i in range(300):
        if e:
            s = e
        else:
            s = get_disk_net_read_write()
        time.sleep(1)
        e = get_disk_net_read_write()
        print "dsk: %s\t%sK\tnet: %s\t%s" % (e['disk_read'] - s['disk_read'],
            (e['disk_write'] - s['disk_write']) * 512.0 / 1024,
            e['net_read'] - s['net_read'],
            e['net_write'] - s['net_write'],)


def clean_collect_logs(func):
    def do(query_name, *args, **kwargs):
        # rm before restart!
        # for question1 part A c
        rm_eventlog_dir()
        rm_local_dirs()
        restart_thrift()
        sync_caches()

        res = func(query_name, *args, **kwargs)

        # for question1 part A c
        collect_eventlogs(query_name)
        return res
    return do


def get_statistics(func):
    def do(*args, **kwargs):
        start_time = time.time()
        start_io = get_disk_net_read_write()

        output = func(*args, **kwargs)

        end_time = time.time()
        end_io = get_disk_net_read_write()
        return {
            "time": end_time - start_time,
            "disk_read": end_io['disk_read'] - start_io['disk_read'],
            "disk_write": end_io['disk_write'] - start_io['disk_write'],
            "net_read": end_io['net_read'] - start_io['net_read'],
            "net_write": end_io['net_write'] - start_io['net_write'],
            "output": output,
        }
    return do


@clean_collect_logs
@get_statistics
def run_spark_query(query_name):
    cmd = "(time /home/ubuntu/software/spark-1.5.0-bin-hadoop2.6/bin/beeline -u jdbc:hive2://group-2-vm1:10000/tpcds_text_db_1_50 -n ubuntu -f $HOME/workload/hive-tpcds-tpch-workload/sample-queries-tpcds/query%s.sql) 2> $HOME/big-data-system/assignment2/output/tpcds_query%s_spark.out" % (query_name, query_name)
    output = execute(cmd, verbose=True)
    return output

def run_spark_query_with_all_partitions(query_name):
    partitions_settings = [5, 10, 100, 200]
    res = []
    for n in partitions_settings:
        cmd = "(time /home/ubuntu/software/spark-1.5.0-bin-hadoop2.6/bin/beeline -u jdbc:hive2://group-2-vm1:10000/tpcds_text_db_1_50 -n ubuntu -f $HOME/workload/hive-tpcds-tpch-workload/sample-queries-tpcds/query%s.sql) 2> $HOME/big-data-system/assignment2/output/tpcds_query%s_spark_partition%d.out" % (query_name, query_name, n)
        custom_conf = "--conf spark.sql.shuffle.partitions=%d" % (n)
        rm_eventlog_dir()
        rm_local_dirs()
        restart_thrift(custom_conf)
        sync_caches()
        res.append(execute(cmd, verbose=True))
    return res

# partitions=10 is the best case
def run_spark_query_with_all_memoryFractions(query_name):
    memoryFractions_settings = ["0.02", "0.05", "0.1", "0.2", "0.4", "0.9"]
    res = []
    for n in memoryFractions_settings:
        cmd = "(time /home/ubuntu/software/spark-1.5.0-bin-hadoop2.6/bin/beeline -u jdbc:hive2://group-2-vm1:10000/tpcds_text_db_1_50 -n ubuntu -f $HOME/workload/hive-tpcds-tpch-workload/sample-queries-tpcds/query%s.sql) 2> $HOME/big-data-system/assignment2/output/tpcds_query%s_spark_memoryFraction%s.out" % (query_name, query_name, n)
        custom_conf = "--conf spark.sql.shuffle.partitions=10 --conf spark.storage.memoryFraction=%s" % (n)
        rm_eventlog_dir()
        rm_local_dirs()
        restart_thrift(custom_conf)
        sync_caches()
        res.append(execute(cmd, verbose=True))
    return res

def find_worker_pid():
    jps_cmd = "jps"
    worker_pid = None
    process_list = execute(jps_cmd, verbose=True)
    for line in process_list.split('\n'):
        toks = line.split(' ')
        if len(toks) > 1 and toks[1].strip() == 'Worker':
            worker_pid = toks[0]
    if worker_pid != None:
        return worker_pid
    else:
        return None

def kill_worker():
    worker_pid = find_worker_pid()
    if worker_pid is None:
        print "Error: failed to find worker pid"
        return
    fail_cmd = "sudo kill -9 %s" % (worker_pid)
    execute(fail_cmd, verbose=True)

def run_spark_query_with_fail_tests(query_name):
    # measure the elapsed time for the query at first
    cmd = "(time /home/ubuntu/software/spark-1.5.0-bin-hadoop2.6/bin/beeline -u jdbc:hive2://group-2-vm1:10000/tpcds_text_db_1_50 -n ubuntu -f $HOME/workload/hive-tpcds-tpch-workload/sample-queries-tpcds/query%s.sql) 2> $HOME/big-data-system/assignment2/output/tpcds_query%s_spark_failtest_no.out" % (query_name, query_name)
    #custom_conf = "--conf spark.sql.shuffle.partitions=10 --conf spark.storage.memoryFraction=0.02"
    custom_conf=""
    rm_eventlog_dir()
    rm_local_dirs()
    restart_spark()
    restart_thrift(custom_conf)
    sync_caches()
    start_time = time.time()
    execute(cmd, verbose=True)
    elapsed_time = time.time() - start_time
    fail_timing25 = elapsed_time / 4.0 + 5
    fail_timing75 = elapsed_time / 4.0 * 3.0 - 5
    print "The task took %f seconds. The 25 failing timing would be %f; and the 75 failing timing would be %f" % (elapsed_time, fail_timing25, fail_timing75)
    # start the failing
    fail_cases = ["_orig", "1-25%", "1-75%", "2-25%", "2-75%"]
    res = []
    for case in fail_cases:
        cmd = "(time /home/ubuntu/software/spark-1.5.0-bin-hadoop2.6/bin/beeline -u jdbc:hive2://group-2-vm1:10000/tpcds_text_db_1_50 -n ubuntu -f $HOME/workload/hive-tpcds-tpch-workload/sample-queries-tpcds/query%s.sql) 2> $HOME/big-data-system/assignment2/output/tpcds_query%s_spark_failtest%s.out" % (query_name, query_name, case)
        custom_conf = "--conf spark.sql.shuffle.partitions=10 --conf spark.storage.memoryFraction=0.02"
        rm_eventlog_dir()
        rm_local_dirs()
        restart_spark()
        restart_thrift(custom_conf)
        sync_caches()
        if "1-" in case:
            if "25%" in case:
                print "Set a timer for failing-task-1 at 25%"
                t = Timer(fail_timing25, sync_caches)
                t.start()
            elif "75%" in case:
                print "Set a timer for failing-task-1 at 75%"
                t = Timer(fail_timing75, sync_caches)
                t.start()
        elif "2-" in case:
            if "25%" in case:
                print "Set a timer for failing-task-2 at 25%"
                t = Timer(fail_timing25, kill_worker)
                t.start()
            elif "75%" in case:
                print "Set a timer for failing-task-2 at 75%"
                t = Timer(fail_timing75, kill_worker)
                t.start()

        res.append(execute(cmd, verbose=True))
    return res

def main():
    parser = argparse.ArgumentParser(description="""
        use this tool to run all spark queries
        """)
    parser.add_argument("-q", "--query", help="tcpds query number", default=12)
    parser.add_argument("-f", "--function", help="run q1-part2&3", default=0)
    args = parser.parse_args()
    if args.function == 0:
        res = run_spark_query(args.query)
        #restart_thrift()
        print [(i, res[i]) for i in res if i != "output"]
        print len(res["output"])
    elif args.function == "1":
        res = run_spark_query_with_fail_tests(args.query)
    elif args.function == "2":
        restart_spark()
        res = run_spark_query_with_all_memoryFractions(args.query)


if __name__ == '__main__':
    main()
