#!/usr/env/python
# -*- coding: utf-8 -*-
import argparse
import random
import string


def get_random_word():
    return get_random_string(random.randint(4, 12))


def get_random_string(length):
    return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))


def main():
    parser = argparse.ArgumentParser(description="generate words files")
    parser.add_argument("-d", "--dir", help="output dir", default="")
    args = parser.parse_args()

    res = get_random_word()
    print res

if __name__ == '__main__':
    main()
