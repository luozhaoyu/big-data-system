#!/bin/sh
dataDir="$HOME/storage/data/zookeeper/"
ssh group-2-vm1 "mkdir -p $dataDir"
ssh group-2-vm2 "mkdir -p $dataDir"
ssh group-2-vm3 "mkdir -p $dataDir"
ssh group-2-vm4 "mkdir -p $dataDir"

ssh group-2-vm1 "mkdir -p $HOME/logs/zookeeper"
ssh group-2-vm2 "mkdir -p $HOME/logs/zookeeper"
ssh group-2-vm3 "mkdir -p $HOME/logs/zookeeper"
ssh group-2-vm4 "mkdir -p $HOME/logs/zookeeper"

ssh group-2-vm1 "mkdir -p $HOME/storage/data/storm"
ssh group-2-vm2 "mkdir -p $HOME/storage/data/storm"
ssh group-2-vm3 "mkdir -p $HOME/storage/data/storm"
ssh group-2-vm4 "mkdir -p $HOME/storage/data/storm"

rsync -avz $HOME/software/zookeeper-3.4.6/ group-2-vm2:$HOME/software/zookeeper-3.4.6/
rsync -avz $HOME/software/zookeeper-3.4.6/ group-2-vm3:$HOME/software/zookeeper-3.4.6/
rsync -avz $HOME/software/zookeeper-3.4.6/ group-2-vm4:$HOME/software/zookeeper-3.4.6/

rsync -avz $HOME/software/apache-storm-0.9.5/ group-2-vm2:$HOME/software/apache-storm-0.9.5/
rsync -avz $HOME/software/apache-storm-0.9.5/ group-2-vm3:$HOME/software/apache-storm-0.9.5/
rsync -avz $HOME/software/apache-storm-0.9.5/ group-2-vm4:$HOME/software/apache-storm-0.9.5/

rsync -avz $HOME/big-data-system/ group-2-vm2:$HOME/big-data-system/
rsync -avz $HOME/big-data-system/ group-2-vm3:$HOME/big-data-system/
rsync -avz $HOME/big-data-system/ group-2-vm4:$HOME/big-data-system/

ssh group-2-vm1 "echo '1' > $dataDir/myid"
ssh group-2-vm2 "echo '2' > $dataDir/myid"
ssh group-2-vm3 "echo '3' > $dataDir/myid"
ssh group-2-vm4 "echo '4' > $dataDir/myid"


