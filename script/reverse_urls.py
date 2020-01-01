#!/usr/bin/env python

from argparse import ArgumentParser
from urllib.parse import urlsplit
import re

def reverse_url(url):
    components = urlsplit(url)
    hostname = components.netloc.partition(':')[0]
    result = '.'.join(hostname.split('.')[::-1])
    if components.path:
        result += components.path
    if components.query:
        result += '.' + components.query
    if components.fragment:
        result += components.fragment

    return re.sub(r'\W', '.', result).lower()


def main():
    parser = ArgumentParser(description="Take a text file of URLs (one per line) \
                             and reverse each URL to use it to reorder the index.")
    parser.add_argument("input", help="Input file containing one URL per line")
    parser.add_argument("output", help="Output file containing one reversed URL per line")
    args = parser.parse_args()

    # Documents dict, where each element maps an id with a document (or url).
    with open(args.input, "r") as in_file, open(args.output, mode='w') as out_file:
        for line in in_file:
            out_file.write(reverse_url(line)+"\n")

if __name__ == "__main__":
    main()
