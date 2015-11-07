#!/bin/sh
output="question$1"
spark-submit --conf "spark.driver.memory=1g" --conf "spark.cores.max=4" --conf "spark.executor.memory=21000m" --conf "spark.task.cpus=1" --conf "spark.eventlog.enabled=true" --class "Question$1" target/scala-2.10/simple-project_2.10-1.0.jar $HOME/random > $output 2>&1
grep -v INFO $output | grep -v WARN > $output.out
cat $output.out
