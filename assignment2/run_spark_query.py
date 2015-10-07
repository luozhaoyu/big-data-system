#!/usr/env/python
# -*- coding: utf-8 -*-
import datetime
import re
import subprocess
import argparse
import time

def execute(cmd, ignored_returns=[], verbose=False):
    result = None
    try:
        if verbose:
            print "RUNNING: %s" % cmd
        result = subprocess.check_output(cmd,
            stderr=subprocess.STDOUT,
            shell=True)
    except subprocess.CalledProcessError as e:
        if e.returncode in ignored_returns:
            return e.output
        else:
            print e.cmd, e.output
            raise e
    return result


def execute_in_background(cmd, output_file):
    output_file = open(output_file, 'a')
    print "execute in background: %s" % cmd
    subprocess.Popen(cmd, shell=True, stdout=output_file, stderr=output_file)


def rm_local_dirs():
    hosts = ["group-2-vm1", "group-2-vm2", "group-2-vm3", "group-2-vm4"]
    for host in hosts:
        execute("ssh ubuntu@%s rm -rf $SPARK_LOCAL_DIRS/*" % host, verbose=True)


def sync_caches():
    hosts = ["group-2-vm1", "group-2-vm2", "group-2-vm3", "group-2-vm4"]
    for host in hosts:
        execute("ssh ubuntu@%s \"sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'\"" % host, verbose=True)


def start_thrift():
    log_file = "thriftserver.out.%s" % (datetime.datetime.isoformat(datetime.datetime.now()))
    cmd = "/usr/lib/jvm/java-1.7.0-openjdk-amd64/bin/java -cp /home/ubuntu/conf/:/home/ubuntu/software/spark-1.5.0-bin-hadoop2.6/lib/spark-assembly-1.5.0-hadoop2.6.0.jar:/home/ubuntu/software/spark-1.5.0-bin-hadoop2.6/lib/datanucleus-rdbms-3.2.9.jar:/home/ubuntu/software/spark-1.5.0-bin-hadoop2.6/lib/datanucleus-api-jdo-3.2.6.jar:/home/ubuntu/software/spark-1.5.0-bin-hadoop2.6/lib/*:/home/ubuntu/conf/:/home/ubuntu/conf/ -Xms1g -Xmx1g -XX:MaxPermSize=256m org.apache.spark.deploy.SparkSubmit --master spark://10.0.1.27:7077 --conf spark.executor.cores=4 --conf spark.executor.memory=21000m --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/home/ubuntu/storage/logs --conf spark.driver.memory=1g --conf spark.task.cpus=1 --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 spark-internal"
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


def restart_thrift():
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
    return start_thrift()


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


def run_spark_query(query_no):
    # rm before restart!
    rm_local_dirs()
    restart_thrift()
    sync_caches()
    start_time = time.time()
    start_io = get_disk_net_read_write()
    cmd = "(time /home/ubuntu/software/spark-1.5.0-bin-hadoop2.6/bin/beeline -u jdbc:hive2://group-2-vm1:10000/tpcds_text_db_1_50 -n ubuntu -f $HOME/workload/hive-tpcds-tpch-workload/sample-queries-tpcds/query%s.sql) 2> $HOME/big-data-system/assignment2/output/tpcds_query%s_spark.out" % (query_no, query_no)
    res = execute(cmd, verbose=True)
    end_time = time.time()
    end_io = get_disk_net_read_write()
    return {
        "time": end_time - start_time,
        "disk_read": end_io['disk_read'] - start_io['disk_read'],
        "disk_write": end_io['disk_write'] - start_io['disk_write'],
        "net_read": end_io['net_read'] - start_io['net_read'],
        "net_write": end_io['net_write'] - start_io['net_write'],
        "output": res,
    }


def main():
    parser = argparse.ArgumentParser(description="""
        use this tool to run all spark queries
        """)
    parser.add_argument("-q", "--query", help="tcpds query number", default=12)
    args = parser.parse_args()
    res = run_spark_query(args.query)
    #restart_thrift()
    print [(i, res[i]) for i in res if i != "output"]
    print len(res["output"])


if __name__ == '__main__':
    main()
