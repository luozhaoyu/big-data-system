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
    - `hadoop jar /home/ubuntu/software/tez-0.7.1-SNAPSHOT-minimal/tez-examples-0.7.1-SNAPSHOT.jar orderedwordcount -Dtez.runtime.io.sort.mb=220 /wcount_input.txt /wcount_output.txt`

### Attention
There maybe collection4 class not found error, please do

1. `sudo apt-get install libcommons-collections4-java`
- add hadoop class path `/usr/share/java/commons-collections4.jar` into `run.sh`
- `source ./run.sh`
- run again

## Hive
1. Start hive console
    - Because run.sh already sets $HIVE_HOME=/home/ubuntu/software/hive-1.2.1 and
    exports $HIVE_HOME/bin to $PATH, directly start hive console by typing
    `hive`
2. Here we set a mysql-user 'hive' and bind it to the master-ip 10.0.1.27 (group-2-vm1)

3. `./hive_workload.sh`
    - The script is placed under the repository (/home/ubuntu/big-data-system)
    - The script will run all the workload (creating a 10Gb database; running a MapReduce job on Hive for the database; and then running a Tez job on Hive for the database). It shows the elasped-time of the MR job and the Tez job.

## Experiment
* `./run_all.sh`
* collect all dstat.csv and outputs from run_all `./collect.sh`

