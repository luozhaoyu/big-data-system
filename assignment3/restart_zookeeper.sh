#!/bin/sh
ZOOKEEPER_HOME=$HOME/software/zookeeper-3.4.6/
start_cmd="$ZOOKEEPER_HOME/bin/zkServer.sh restart"

ssh group-2-vm1 "$start_cmd"
ssh group-2-vm2 "$start_cmd"
ssh group-2-vm3 "$start_cmd"
ssh group-2-vm4 "$start_cmd"

ssh group-2-vm1 "$ZOOKEEPER_HOME/bin/zkServer.sh status"
ssh group-2-vm2 "$ZOOKEEPER_HOME/bin/zkServer.sh status"
ssh group-2-vm3 "$ZOOKEEPER_HOME/bin/zkServer.sh status"
ssh group-2-vm4 "$ZOOKEEPER_HOME/bin/zkServer.sh status"
