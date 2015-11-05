#!/usr/env/python
# -*- coding: utf-8 -*-
import os
import argparse
import random
import string


def get_random_word():
    return get_random_string(random.randint(4, 12))


def get_random_string(length):
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(length))

def write_random_data(filepath):
    lines = []
    for i in range(random.randint(80, 150)):
        lines.append((random.randint(2, 20), get_random_string(2)))

    with open(filepath, 'w') as f:
        for line in lines:
            f.write("%s\t%s\n" % (line[0], line[1]))

def write_random_folder(folderpath, num=200):
    try:
        os.mkdir(folderpath)
    except OSError as e:
        print e

    for i in range(num):
        filepath = os.path.join(folderpath, get_random_string(4))
        write_random_data(filepath)


def main():
    parser = argparse.ArgumentParser(description="generate words files")
    parser.add_argument("-d", "--dir", help="output dir", default="random_words")
    args = parser.parse_args()

    write_random_folder(args.dir)

if __name__ == '__main__':
    main()
