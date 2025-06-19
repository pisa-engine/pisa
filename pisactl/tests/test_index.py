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
from typing import Final
from unittest.mock import MagicMock, sentinel

from pydantic import ValidationError
import pytest
import pisactl.command.index
from pisactl.command.index import AddIndexArgs, IndexArgs, index, add_index
from pisactl.metadata import Analyzer, Block, FixedBlock, VariableBlock
from pisactl.scorer import BM25, Scorer
from pisactl.source import CiffSource, IrDatasetsSource, ParseFormat, StdinSource

DEFAULT_SCORER: Final[BM25] = BM25(name="bm25", k1=0.9, b=0.4)


def test_index_args():
    with pytest.raises(ValidationError):
        IndexArgs.parse(argparse.Namespace(source="stdin", scorer="bm25"))

    with pytest.raises(ValueError) as err:
        IndexArgs.parse(
            argparse.Namespace(
                alias="default", output="/output", force=False, encoding="block_simdbp"
            )
        )
    assert str(err.value) == "missing source"

    with pytest.raises(ValueError) as err:
        assert IndexArgs.parse(
            argparse.Namespace(
                alias="default",
                source="stdin",
                output="/output",
                force=False,
                encoding="block_simdbp",
            )
        )
    assert str(err.value) == "missing scorer"

    # missing format
    with pytest.raises(ValidationError) as err:
        assert IndexArgs.parse(
            argparse.Namespace(
                alias="default",
                scorer="bm25",
                source="stdin",
                output="/output",
                force=False,
                encoding="block_simdbp",
            )
        )

    assert IndexArgs.parse(
        argparse.Namespace(
            alias="default",
            output="/output",
            force=False,
            encoding="block_simdbp",
            block_size=32,
            scorer="bm25",
            source="stdin",
            format="jsonl",
        )
    ) == IndexArgs(
        alias="default",
        output="/output",
        force=False,
        encoding="block_simdbp",
        source=StdinSource(format=ParseFormat.JSONL, analyzer=Analyzer()),
        scorer=DEFAULT_SCORER,
        block=FixedBlock(size=32),
    )

    assert IndexArgs.parse(
        argparse.Namespace(
            alias="default",
            output="/output",
            force=False,
            encoding="block_simdbp",
            block_size=32,
            scorer="bm25",
            source="ciff",
            input="/input",
        )
    ) == IndexArgs(
        alias="default",
        output="/output",
        force=False,
        encoding="block_simdbp",
        source=CiffSource(input=pathlib.Path("/input")),
        scorer=DEFAULT_SCORER,
        block=FixedBlock(size=32),
    )

    assert IndexArgs.parse(
        argparse.Namespace(
            alias="default",
            output="/output",
            force=False,
            encoding="block_simdbp",
            block_size=32,
            scorer="bm25",
            source="ir-datasets",
            name="wikir/en1k",
            content_fields=["docid", "text"],
        )
    ) == IndexArgs(
        alias="default",
        output="/output",
        force=False,
        encoding="block_simdbp",
        source=IrDatasetsSource(
            name="wikir/en1k", content_fields=["docid", "text"], analyzer=Analyzer()
        ),
        scorer=DEFAULT_SCORER,
        block=FixedBlock(size=32),
    )


@pytest.mark.parametrize(
    "alias,force,encoding,format,analyzer,scorer,block,quantize",
    [
        (
            "default",
            False,
            "block_simdbp",
            ParseFormat.JSONL,
            Analyzer(),
            DEFAULT_SCORER,
            FixedBlock(size=32),
            None,
        ),
        (
            "qmx",
            True,
            "block_qmx",
            ParseFormat.PLAINTEXT,
            Analyzer(tokenizer="whitespace", strip_html=False, token_filters=["lowercase"]),
            DEFAULT_SCORER,
            VariableBlock.model_validate({"lambda": 2.1}),
            8,
        ),
    ],
)
def test_index_stdin(
    alias: str,
    force: bool,
    encoding: str,
    parse_format: ParseFormat,
    analyzer: Analyzer,
    scorer: Scorer,
    block: Block,
    quantize: int | None,
):
    tools = MagicMock(
        parse_pipe=MagicMock(return_value=sentinel.meta),
        invert_forward_index=MagicMock(return_value=sentinel.meta),
        compress=MagicMock(),
    )
    index(
        tools,
        IndexArgs(
            alias=alias,
            output="/output",
            force=force,
            encoding=encoding,
            source=StdinSource(format=parse_format, analyzer=analyzer),
            scorer=scorer,
            block=block,
            quantize=quantize,
        ),
    )
    tools.parse_pipe.assert_called_once_with(
        "/output",
        analyzer=analyzer,
        fmt=parse_format.value,
        force=force,
    )
    tools.invert_forward_index.assert_called_once_with(sentinel.meta)
    tools.compress.assert_called_once_with(
        sentinel.meta,
        encoding,
        scorer=scorer,
        block=block,
        alias=alias,
        quantization=quantize,
    )


@pytest.mark.parametrize(
    "alias,force,encoding,name,content_fields,analyzer,scorer,block,quantize",
    [
        (
            "default",
            False,
            "block_simdbp",
            "wikir/en1k",
            ["docid", "text"],
            Analyzer(),
            DEFAULT_SCORER,
            FixedBlock(size=32),
            None,
        ),
        (
            "qmx",
            True,
            "block_qmx",
            "clueweb09b",
            ["content"],
            Analyzer(tokenizer="whitespace", strip_html=False, token_filters=["lowercase"]),
            DEFAULT_SCORER,
            VariableBlock.model_validate({"lambda": 2.1}),
            8,
        ),
    ],
)
def test_index_ir_datasets(
    alias: str,
    force: bool,
    encoding: str,
    name: str,
    content_fields: list[str],
    analyzer: Analyzer,
    scorer: Scorer,
    block: Block,
    quantize: int | None,
):
    tools = MagicMock(
        parse_ir_datasets=MagicMock(return_value=sentinel.meta),
        invert_forward_index=MagicMock(return_value=sentinel.meta),
        compress=MagicMock(),
    )
    index(
        tools,
        IndexArgs(
            alias=alias,
            output="/output",
            force=force,
            encoding=encoding,
            source=IrDatasetsSource(analyzer=analyzer, name=name, content_fields=content_fields),
            scorer=scorer,
            block=block,
            quantize=quantize,
        ),
    )
    tools.parse_ir_datasets.assert_called_once_with(
        name=name,
        analyzer=analyzer,
        output_dir="/output",
        content_fields=content_fields,
        force=force,
    )
    tools.invert_forward_index.assert_called_once_with(sentinel.meta)
    tools.compress.assert_called_once_with(
        sentinel.meta,
        encoding,
        scorer=scorer,
        block=block,
        alias=alias,
        quantization=quantize,
    )


@pytest.mark.parametrize(
    "alias,force,encoding,input,scorer,block,quantize",
    [
        (
            "default",
            False,
            "block_simdbp",
            "/input",
            DEFAULT_SCORER,
            FixedBlock(size=32),
            None,
        ),
        (
            "qmx",
            True,
            "block_qmx",
            "/input",
            DEFAULT_SCORER,
            VariableBlock.model_validate({"lambda": 2.1}),
            8,
        ),
    ],
)
def test_index_ciff(
    alias: str,
    force: bool,
    encoding: str,
    input_path: str,
    scorer: Scorer,
    block: Block,
    quantize: int | None,
):
    tools = MagicMock(
        ciff_to_pisa=MagicMock(return_value=sentinel.meta),
        compress=MagicMock(),
    )
    index(
        tools,
        IndexArgs(
            alias=alias,
            output="/output",
            force=force,
            encoding=encoding,
            source=CiffSource(input=pathlib.Path(input_path)),
            scorer=scorer,
            block=block,
            quantize=quantize,
        ),
    )
    tools.ciff_to_pisa.assert_called_once_with(input_path, "/output", force=force)
    tools.compress.assert_called_once_with(
        sentinel.meta,
        encoding,
        scorer=scorer,
        block=block,
        alias=alias,
        quantization=quantize,
    )


@pytest.mark.parametrize(
    "alias,encoding,scorer,block,quantize",
    [
        ("default", "block_simdbp", DEFAULT_SCORER, FixedBlock(size=32), None),
        ("qmx", "block_qmx", DEFAULT_SCORER, VariableBlock.model_validate({"lambda": 2.1}), 8),
    ],
)
def test_add_index(
    alias: str,
    encoding: str,
    scorer: Scorer,
    block: Block,
    quantize: int | None,
    monkeypatch: pytest.MonkeyPatch,
):
    load = MagicMock(return_value=sentinel.meta)
    monkeypatch.setattr(pisactl.command.index.CollectionMetadata, "load", load)
    tools = MagicMock(
        ciff_to_pisa=MagicMock(return_value=sentinel.meta),
        compress=MagicMock(),
    )
    add_index(
        tools,
        AddIndexArgs(
            workdir="/workdir",
            alias=alias,
            encoding=encoding,
            scorer=scorer,
            block=block,
            quantize=quantize,
        ),
    )
    load.assert_called_once_with(pathlib.Path("/workdir"))
    tools.compress.assert_called_once_with(
        sentinel.meta,
        encoding,
        scorer=scorer,
        block=block,
        alias=alias,
        quantization=quantize,
    )
