#!/usr/bin/env python
import os
from argparse import ArgumentParser

TREC_FORMAT = "<DOC>\n<DOCNO>{}</DOCNO>\n<TEXT>\n{}</TEXT>\n</DOC>\n"

def compute_paths(path):
    paths = []
    for root, _, fnames in os.walk(path):
        paths.extend([os.path.join(root, fname) for fname in fnames])
    return paths

def to_trec(paths, output):
    with open(output, "w") as f:
        for path in paths:
            f.write(TREC_FORMAT.format(path, open(path).read()))

def main():
    parser = ArgumentParser(description="Format a set of files (where each one is a document) to a \
                                        TREC file. Take into account that the relative path of each \
                                        input file is used as the document ID.")
    parser.add_argument("input", type=str, help="Input directory")
    parser.add_argument("output", type=str, help="Output filename")
    args = parser.parse_args()
    print("Formatting '{}' to TREC...".format(str(args.input)))
    to_trec(compute_paths(args.input), args.output)

if __name__ == "__main__":
    main()