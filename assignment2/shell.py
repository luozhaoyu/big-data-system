#!/usr/env/python
# -*- coding: utf-8 -*-
import subprocess

def execute(cmd, ignored_returns=[], verbose=False):
    result = None
    try:
        if verbose:
            print "RUNNING: %s" % cmd
        result = subprocess.check_output(cmd,
            stderr=subprocess.STDOUT,
            shell=True)
    except subprocess.CalledProcessError as e:
        if e.returncode in ignored_returns:
            return e.output
        else:
            print e.cmd, e.output
            raise e
    return result


def execute_in_background(cmd, output_file):
    output_file = open(output_file, 'a')
    print "execute in background: %s" % cmd
    subprocess.Popen(cmd, shell=True, stdout=output_file, stderr=output_file)


def main():
    pass


if __name__ == '__main__':
    main()
