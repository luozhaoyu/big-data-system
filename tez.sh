#!/bin/sh
wget -c -P ~/software http://pages.cs.wisc.edu/~akella/CS838/F15/assignment1/tez-0.7.1-SNAPSHOT.tar.gz
wget -c -P ~/software http://pages.cs.wisc.edu/~akella/CS838/F15/assignment1/tez-0.7.1-SNAPSHOT-minimal.tar.gz
hadoop dfs -mkdir -p /apps/tez-0.7.1-SNAPSHOT
hadoop dfs -copyFromLocal ~/software/tez-0.7.1-SNAPSHOT.tar.gz /apps/tez-0.7.1-SNAPSHOT/
hadoop dfs -ls /apps/tez-0.7.1-SNAPSHOT
tar -xf ~/software/tez-0.7.1-SNAPSHOT-minimal.tar.gz -C ~/software/tez
