# Copyright 2025 PISA Developers
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import argparse
import os
from typing import Literal

import pydantic
from ruamel.yaml import sys

from pisactl.command.index import AddIndexArgs, IndexArgs, IndexDirExists, index, add_index
from pisactl.command.query import query
from pisactl.command.query_meta import query_meta
from pisactl.metadata import AliasNotFound, IndexResolutionError, MetadataNotFound
from pisactl.tools import ToolError, Tools

Command = (
    Literal["index"]
    | Literal["add-index"]
    | Literal["reorder-documents"]
    | Literal["query"]
    | Literal["meta"]
)

ALGORITHMS = ["or", "and", "ranked_or", "ranked_and", "wand", "block_max_wand", "maxscore"]
TOKENIZERS = ["english", "whitespace"]
TOKEN_FILTERS = ["lowercase", "porter2", "krovetz"]


def add_analyzer_arguments(parser: argparse.ArgumentParser):
    group = parser.add_argument_group("analyzer")
    group.add_argument("--tokenizer", default="english", choices=TOKENIZERS)
    group.add_argument(
        "-F", "--token-filters", nargs="+", default=["lowercase", "porter2"], choices=TOKEN_FILTERS
    )
    group.add_argument("-H", "--strip-html", action="store_true", help="Strip HTML")


def add_compression_arguments(parser: argparse.ArgumentParser, *, require_alias: bool):
    group = parser.add_argument_group("compression")
    group.add_argument("--encoding", default="block_simdbp")
    group.add_argument("--scorer", default="bm25")
    block_group = group.add_mutually_exclusive_group()
    block_group.add_argument("--block-size", default="64", type=int, help="Skip-list block size")
    block_group.add_argument(
        "--lambda", dest="lambda_", type=float, help="Parameter for variable block computation"
    )
    group.add_argument("--quantize", type=int, help="Quantize using this many bits")
    parser.add_argument(
        "--alias",
        help="Compressed index alias",
        required=require_alias,
        default=None if require_alias else "default",
    )


def ir_datasets_parser(parser: argparse.ArgumentParser):
    parser.set_defaults(source="ir-datasets")
    parser.add_argument("name")
    parser.add_argument("--content-fields", nargs="+", default="content")
    parser.add_argument("-o", "--output", required=True)
    parser.add_argument(
        "-f", "--force", help="Proceed even if output dir exists", action="store_true"
    )
    add_compression_arguments(parser, require_alias=False)
    add_analyzer_arguments(parser)


def stdin_parser(parser: argparse.ArgumentParser):
    parser.set_defaults(source="stdin")
    parser.add_argument("-o", "--output", required=True)
    parser.add_argument(
        "-f", "--force", help="Proceed even if output dir exists", action="store_true"
    )
    parser.add_argument("--format", required=True)
    add_compression_arguments(parser, require_alias=False)
    add_analyzer_arguments(parser)


def ciff_parser(parser: argparse.ArgumentParser):
    parser.set_defaults(source="ciff")
    parser.add_argument("-i", "--input", required=True)
    parser.add_argument("-o", "--output", required=True)
    parser.add_argument(
        "-f", "--force", help="Proceed even if output dir exists", action="store_true"
    )
    add_compression_arguments(parser, require_alias=False)


def add_index_parser(parser: argparse.ArgumentParser):
    parser.set_defaults(command="add-index")
    parser.add_argument("-w", "--workdir", default=".")
    add_compression_arguments(parser, require_alias=True)


def index_parser(parser: argparse.ArgumentParser):
    parser.set_defaults(command="index")
    subparsers = parser.add_subparsers(title="source", required=True)
    ir_datasets_parser(subparsers.add_parser("ir-datasets", help="Build an index from ir-datasets"))
    ciff_parser(subparsers.add_parser("ciff", help="Build an index from CIFF"))
    stdin_parser(subparsers.add_parser("stdin", help="Build an from standard input"))


def query_parser(parser: argparse.ArgumentParser):
    parser.set_defaults(command="query")
    parser.add_argument("-w", "--workdir", default=".")
    parser.add_argument("--alias", help="Compressed index alias", default="default")
    parser.add_argument("-k", type=int, help="Number of results to retrieve", default=10)
    parser.add_argument(
        "--algorithm", help="Retrieval algorithm", default="block_max_wand", choices=ALGORITHMS
    )
    parser.add_argument("--benchmark", action="store_true", default=False)
    parser.add_argument(
        "--weighted",
        action="store_true",
        default=False,
        help="Add weight to repeated query terms",
    )


def meta_parser(parser: argparse.ArgumentParser):
    parser.set_defaults(command="meta")
    parser.add_argument("-w", "--workdir", default=".")
    subparsers = parser.add_subparsers(title="subcommand", required=True)

    subparsers.add_parser("print", help="Print entire metadata")
    parser.set_defaults(subcommand="print")

    aliases = subparsers.add_parser("aliases", help="List compressed index aliases")
    aliases.set_defaults(subcommand="aliases")

    alias = subparsers.add_parser("alias", help="List metadata for alias")
    alias.set_defaults(subcommand="alias")
    alias.add_argument("alias")


def main() -> None:
    parser = argparse.ArgumentParser("pisactl")
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print each executed subprocess command",
    )
    parser.add_argument(
        "--bin",
        dest="bin_dir",
        help="Directory with PISA binaries",
        default=os.getenv("PISA_BIN"),
    )
    subparsers = parser.add_subparsers(title="commands", required=True)
    index_parser(subparsers.add_parser("index", help="Build an index"))
    add_index_parser(subparsers.add_parser("add-index", help="Add another index"))
    query_parser(subparsers.add_parser("query", help="Query an index"))
    meta_parser(subparsers.add_parser("meta", help="Read metadata"))
    args = parser.parse_args()

    try:
        tools = Tools(args.bin_dir, args.verbose)

        command: Command = args.command
        match command:
            case "index":
                index(tools, IndexArgs.parse(args))
            case "add-index":
                add_index(tools, AddIndexArgs.parse(args))
            case "query":
                query(tools, args)
            case "meta":
                query_meta(args)
    except (
        MetadataNotFound,
        AliasNotFound,
        ToolError,
        IndexResolutionError,
        IndexDirExists,
        pydantic.ValidationError,
    ) as err:
        print(f"ERROR: {err}", file=sys.stderr)
        sys.exit(1)
