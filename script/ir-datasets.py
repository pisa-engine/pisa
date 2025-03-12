#!/usr/bin/env python3

import argparse
import json
import os

import ir_datasets as ir
from ir_datasets.log import sys


def _parser():
    parser = argparse.ArgumentParser("ir-datasets")
    parser.add_argument("dataset")
    parser.add_argument(
        "-f",
        "--content-fields",
        nargs="+",
        required=True,
        help="list of fields to use as indexed content",
    )
    return parser


def parse_dataset(dataset_name, fields: list[str]):
    dataset = ir.load(dataset_name)
    for doc in dataset.docs_iter():
        content = " ".join(getattr(doc, field) for field in fields)
        print(json.dumps({"title": doc.doc_id, "content": content}))


def main():
    try:
        parser = _parser()
        args = parser.parse_args()
        parse_dataset(args.dataset, args.content_fields)
        sys.stdout.flush()
    except BrokenPipeError:
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())


if __name__ == "__main__":
    main()
