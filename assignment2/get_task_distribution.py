#!/usr/env/python
# -*- coding: utf-8 -*-
from shell import execute, execute_in_background


def get_tasks_launch_finish_time(input_file):
    cmd = 'grep SparkListenerTaskEnd %s | grep -ioP "Launch Time.*?,|Finish Time.*?,"' % input_file
    raw = execute(cmd)

    res = []
    for i in raw.split('\n'):
        if i:
            res.append(int(i.strip(',').split(':')[-1][:-3]))
    return res


def get_distribution(time_pairs):
    base_time = min(time_pairs)
    res = [0] * (max(time_pairs) - base_time + 2)
    for i in range(len(time_pairs) / 2):
        start = time_pairs[2 * i] - base_time
        end = time_pairs[2 * i + 1] - base_time
        assert end >= start
        for j in range(start, end + 1):
            res[j] += 1
    return res


def main():
    import sys
    res = get_tasks_launch_finish_time(sys.argv[1])
    distr = get_distribution(res)
    for i in distr:
        print i


if __name__ == '__main__':
    main()
