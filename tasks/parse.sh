#!/bin/sh
output="tasks.txt"
find $1 | xargs grep -iP --color "task_finished|task_started" > $output
grep -v fields $output | grep -oiP --color 'jhist:|"type.*?,|taskid.*?,|finishtime.*?,|starttime.*?,|tasktype.*?,' > task_info.txt
