ssh ubuntu@group-2-vm1 "sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'"
ssh ubuntu@group-2-vm2 "sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'"
ssh ubuntu@group-2-vm3 "sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'"
ssh ubuntu@group-2-vm4 "sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'"
