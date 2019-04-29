#!/usr/bin/env python

import argparse
import sys

def validate(ndoc, source):
    levels = {}
    for line in source:
        level, iterations, ll, lr, rl, rr = line.split()
        levels[level] = levels.get(level, [False] * ndoc)
        for n in list(range(int(ll), int(lr))) + list(range(int(rl), int(rr))):
            if levels[level][n]:
                print("Failed!", file=sys.stderr)
                return
            levels[level][n] = True

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Validate BP config file.')
    parser.add_argument('ndoc', type=int,
                        help='Number of document in collection.')
    parser.add_argument('input',
                        type=argparse.FileType('r'), default=sys.stdin,
                        help='Net config file')
    args = parser.parse_args()
    validate(args.ndoc, args.input)
