#!/bin/sh
cd ~
mkdir -p ~/logs/apps
mkdir -p ~/logs/hadoop
mkdir -p ~/software
mkdir -p ~/storage/data/local/nm
mkdir -p ~/storage/data/local/tmp
mkdir -p ~/storage/hdfs/hdfs_dn_dirs
mkdir -p ~/storage/hdfs/hdfs_nn_dir
mkdir -p ~/workload
ln -sf ~/big-data-system/conf/ ~
ln -sf ~/big-data-system/instances ~
ln -sf ~/big-data-system/run.sh ~
wget -c -P ~/software http://apache.arvixe.com/hadoop/common/hadoop-2.6.0/hadoop-2.6.0.tar.gz
echo -n "please untar Hadoop now"
