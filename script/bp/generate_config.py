#!/usr/bin/env python

import argparse
import sys

def generate_recursive(lvl, first, last, out):
    if last - first < 2:
        return
    left_range = (first, first + (last - first) // 2)
    right_range = (left_range[1], last)
    print(lvl, 20, left_range[0], left_range[1],
          right_range[0], right_range[1],
          0, file=out)
    generate_recursive(lvl + 1, left_range[0], left_range[1], out)
    generate_recursive(lvl + 1, right_range[0], right_range[1], out)

def generate(ndoc, out):
    """
    Generates nodes (one per line) in format:
    LVL I FL FR SL SR C
    where
        - LVL is integer defining level (depth) in recursion tree
        - I is iteration count
        - FL first range's beginning
        - FR first range's end
        - SL second range's beginning
        - SR second range's end
        - 0 or 1 (cache gains or not)
    """
    generate_recursive(0, 0, ndoc, out);

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate BP config file.')
    parser.add_argument('ndoc', type=int,
                        help='Number of document in collection.')
    parser.add_argument('-o', '--output',
                        type=argparse.FileType('w'), default=sys.stdout,
                        help='Ouptut config file (if absent, write to stdout)')
    args = parser.parse_args()
    generate(args.ndoc, args.output)
