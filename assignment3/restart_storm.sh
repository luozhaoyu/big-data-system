#!/bin/sh
ssh group-2-vm1 "$HOME/software/apache-storm-0.9.5/bin/storm nimbus > $HOME/logs/nimbus.log 2>&1 &"

ssh group-2-vm1 "$HOME/software/apache-storm-0.9.5/bin/storm supervisor > $HOME/logs/supervisor.log 2>&1 &"
ssh group-2-vm2 "$HOME/software/apache-storm-0.9.5/bin/storm supervisor > $HOME/logs/supervisor.log 2>&1 &"
ssh group-2-vm3 "$HOME/software/apache-storm-0.9.5/bin/storm supervisor > $HOME/logs/supervisor.log 2>&1 &"
ssh group-2-vm4 "$HOME/software/apache-storm-0.9.5/bin/storm supervisor > $HOME/logs/supervisor.log 2>&1 &"

ssh group-2-vm1 "$HOME/software/apache-storm-0.9.5/bin/storm ui > $HOME/logs/storm-ui.log 2>&1 &"
