# big-data-system
Big Data System

## Yarn
1. environments
    * setup ssh ForwardAgent to access master and slaves
    * config `/etc/hosts`
    * config `/home/ubuntu/*`, replace the MASTER_IP with `group-2-vm1`
- `sudo apt-get update --fix-missing`
- `sudo apt-get install openjdk-7-jdk pdsh`
- `sudo apt-get install git`
    * `git clone https://github.com/luozhaoyu/big-data-system.git`
- `./mkdirs_configs.sh`
    * it would download hadoop
- untar hadoop
- `source run.sh`
- `hadoop namenode -format`
- `start_all`
- monitor and debug
    * `hadoop jar ~/software/hadoop-2.6.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.0.jar pi 1 2`
    * `jps`
    * HDFS <http://130.127.132.234:50070>
    * YARN <http://130.127.132.234:8088>
    * MapReduce job history <http://130.127.132.234:19888>
    * application history <http://130.127.132.234:8188>

## Tez
1. `./tez.sh`
- test
    1. `hadoop dfs -copyFromLocal ~/wcount_input.txt /`
    - `hadoop jar /home/ubuntu/software/tez/tez-examples-0.7.1-SNAPSHOT.jar orderedwordcount -Dtez.runtime.io.sort.mb=220 /wcount_input.txt /wcount_output.txt`
