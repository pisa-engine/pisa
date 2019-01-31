#!/usr/bin/env python

import argparse
import json
import subprocess
import sys
from os import walk
from os import path

def accumulate_headers(directory, header_filter = lambda f: True):
    includes = []
    for (_, _, filenames) in walk(directory):
        includes.extend([{"name": filename} for filename in filenames
                         if header_filter(filename)])
    return includes

def run_dir(directory, line_filters):
    for (r, _, filenames) in walk(directory):
        for f in filenames:
            cmdline = 'clang-tidy -line-filter="{}" {}'.format(line_filters,
                                                               path.join(r, f))
            print(cmdline)
            result = subprocess.run(cmdline, shell=True, check=True)
            if result.returncode != 0:
                sys.exit(result.returncode)

def tidy(root, exclude):
    headers = accumulate_headers(path.join(root, 'include/pisa'),
                                 lambda f: exclude is None or f not in exclude)
    line_filters = json.dumps(headers)
    line_filters = line_filters.replace(r'"', r"'")
    run_dir(path.join(root, 'src'), line_filters)
    run_dir(path.join(root, 'test'), line_filters)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run clang-tidy for the project.')
    parser.add_argument('--exclude', nargs='+', help='Header files to exclude')
    parser.add_argument('root', help='Root directory')
    args = parser.parse_args()
    tidy(args.root, args.exclude)
