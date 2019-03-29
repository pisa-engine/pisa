#!/usr/bin/env python

import argparse
import sys

def make_net(left, right, iterations, level, minsize, out):
    mid = (left + right) // 2
    q1 = (left + mid) // 2
    q3 = (mid + right) // 2

    print(level, iterations, left, mid, mid, right, 0, file=out)
    print(level + 1, iterations, left, q1, q1, mid, 0, file=out)
    print(level + 1, iterations, mid, q3, q3, right, 0, file=out)
    print(level + 2, iterations, left, q1, mid, q3, 0, file=out)
    print(level + 2, iterations, q1, mid, q3, right, 0, file=out)
    print(level + 3, iterations, left, q1, q1, mid, 0, file=out)
    print(level + 3, iterations, mid, q3, q3, right, 0, file=out)
    print(level + 4, iterations, left, q1, q3, right, 0, file=out)
    print(level + 4, iterations, q1, mid, mid, q3, 0, file=out)

    if right - left > minsize:
        make_net(left, mid, iterations, level + 5, minsize, out)
        make_net(mid, right, iterations, level + 5, minsize, out)

def generate(ndoc, out, iterations, minsize):
    make_net(0, ndoc, iterations, 0, minsize, out)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate BP config file.')
    parser.add_argument('--iterations', type=int, default=10,
                        help='Number of iterations.')
    parser.add_argument('--minsize', type=int, default=20,
                        help='Size limit for recursion.')
    parser.add_argument('ndoc', type=int,
                        help='Number of document in collection.')
    parser.add_argument('-o', '--output',
                        type=argparse.FileType('w'), default=sys.stdout,
                        help='Ouptut config file (if absent, write to stdout)')
    args = parser.parse_args()
    generate(args.ndoc, args.output, args.iterations, args.minsize)
