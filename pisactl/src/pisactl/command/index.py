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
import pathlib

import pydantic
from pisactl import scorer
from pisactl.metadata import Analyzer, CollectionMetadata, FixedBlock, VariableBlock
from pisactl.scorer import Scorer
from pisactl.source import CiffSource, IrDatasetsSource, Source, StdinSource, VectorsSource
from pisactl.tools import Tools


class IndexDirExists(Exception):
    def __init__(self, path: pathlib.Path):
        self.path = path

    def __str__(self) -> str:
        return f"index dir already exists ({self.path}), use --force to overwrite"


def _block(args: dict):
    if "lambda_" in args and args["lambda_"] is not None:
        return VariableBlock.model_validate({"lambda": args["lambda_"]})
    return FixedBlock(size=args["block_size"])


def _mkdir(path: pathlib.Path, force: bool):
    try:
        path.mkdir(exist_ok=force)
    except FileExistsError:
        raise IndexDirExists(path)


class CommonIndexArgs(pydantic.BaseModel):
    alias: str
    encoding: str
    quantize: int | None = None
    scorer: Scorer
    block: FixedBlock | VariableBlock


class IndexArgs(CommonIndexArgs):
    output: str
    source: Source
    force: bool
    block: FixedBlock | VariableBlock

    @staticmethod
    def parse(args: argparse.Namespace) -> "IndexArgs":
        argdict = vars(args)

        if "source" not in argdict:
            raise ValueError("missing source")
        if "scorer" not in argdict:
            raise ValueError("missing scorer")

        match argdict["source"]:
            case "ciff":
                source = CiffSource.model_validate(argdict)
            case "stdin":
                source = StdinSource.model_validate(
                    {**argdict, "analyzer": Analyzer.model_validate(argdict)}
                )
            case "ir-datasets":
                source = IrDatasetsSource.model_validate(
                    {**argdict, "analyzer": Analyzer.model_validate(argdict)}
                )
            case _:
                raise ValueError(f"invalid source: {argdict['source']}")

        return IndexArgs.model_validate(
            {**argdict, "source": source, "scorer": scorer.resolve(args), "block": _block(argdict)}
        )


class AddIndexArgs(CommonIndexArgs):
    workdir: str

    @staticmethod
    def parse(args: argparse.Namespace) -> "AddIndexArgs":
        argdict = vars(args)
        return AddIndexArgs.model_validate(
            {**argdict, "scorer": scorer.resolve(args), "block": _block(argdict)}
        )


def index(tools: Tools, args: IndexArgs):
    _mkdir(pathlib.Path(args.output), force=args.force)
    match args.source:
        case CiffSource() as source:
            assert source.input is not None
            meta = tools.ciff_to_pisa(str(source.input), args.output)
        case IrDatasetsSource() as source:
            meta = tools.parse_ir_datasets(
                name=source.name,
                analyzer=source.analyzer,
                output_dir=args.output,
                content_fields=source.content_fields,
            )
            meta = tools.invert_forward_index(meta)
        case StdinSource() as source:
            meta = tools.parse_pipe(args.output, analyzer=source.analyzer, fmt=source.format.value)
            meta = tools.invert_forward_index(meta)
        case VectorsSource() as source:
            raise RuntimeError("not implemented")

    meta = tools.compress(
        meta,
        args.encoding,
        scorer=args.scorer,
        block=args.block,
        alias=args.alias,
        quantization=args.quantize,
    )
    meta.dump()


def add_index(tools: Tools, args: AddIndexArgs):
    meta = CollectionMetadata.load(pathlib.Path(args.workdir))
    meta = tools.compress(
        meta,
        args.encoding,
        scorer=args.scorer,
        block=args.block,
        alias=args.alias,
        quantization=args.quantize,
    )
    meta.dump()
