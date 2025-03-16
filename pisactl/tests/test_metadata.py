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


from pathlib import Path

from pydantic_yaml import parse_yaml_raw_as, to_yaml_str

import pytest
from pisactl import metadata, scorer


def test_uncompressed_index():
    uncompressed_index = metadata.UncompressedIndex(
        documents=Path("/path/to/documents"),
        values=Path("/path/to/values"),
        sizes=Path("/path/to/sizes"),
    )
    yaml = """documents: /path/to/documents
sizes: /path/to/sizes
values: /path/to/values
"""
    assert to_yaml_str(uncompressed_index) == yaml
    assert parse_yaml_raw_as(metadata.UncompressedIndex, yaml) == uncompressed_index


@pytest.mark.parametrize(
    "wdata,yaml",
    [
        (
            metadata.WandData(
                block=metadata.FixedBlock(size=64),
                scorer=scorer.BM25(name="bm25", k1=1.0, b=2.0),
            ),
            """block:
  size: 64
path: wdata:size=64:bm25:b=2.0:k1=1.0
scorer:
  b: 2.0
  k1: 1.0
  name: bm25
""",
        ),
        (
            metadata.WandData(
                block=metadata.FixedBlock(size=64),
                scorer=scorer.BM25(name="bm25", k1=1.0, b=2.0),
                quantization=8,
            ),
            """block:
  size: 64
path: wdata:size=64:bm25:b=2.0:k1=1.0:quantization=8
quantization: 8
scorer:
  b: 2.0
  k1: 1.0
  name: bm25
""",
        ),
        (
            metadata.WandData(
                path=Path("/path/to/wdata"),
                block=metadata.FixedBlock(size=64),
                scorer=scorer.BM25(name="bm25", k1=1.0, b=2.0),
            ),
            """block:
  size: 64
path: /path/to/wdata
scorer:
  b: 2.0
  k1: 1.0
  name: bm25
""",
        ),
        (
            metadata.WandData(
                path=Path("/path/to/wdata"),
                block=metadata.VariableBlock.model_validate({"lambda": 8.7}),
                scorer=scorer.QLD(name="qld", mu=1000),
                quantization=8,
            ),
            """block:
  lambda: 8.7
path: /path/to/wdata
quantization: 8
scorer:
  mu: 1000.0
  name: qld
""",
        ),
    ],
)
def test_wand_data(wdata: metadata.WandData, yaml: str):
    assert to_yaml_str(wdata, by_alias=True, exclude_none=True) == yaml
    assert parse_yaml_raw_as(metadata.WandData, yaml) == wdata


@pytest.mark.parametrize(
    "index,yaml",
    [
        (
            metadata.CompressedIndexFile(encoding="block_simdbp"),
            """encoding: block_simdbp
path: inv:block_simdbp
""",
        ),
        (
            metadata.CompressedIndexFile(path=Path("/path/to/index"), encoding="block_simdbp"),
            """encoding: block_simdbp
path: /path/to/index
""",
        ),
        (
            metadata.CompressedIndexFile(
                encoding="block_simdbp",
                quantization=8,
                scorer=scorer.BM25(name="bm25", k1=1.0, b=2.0),
            ),
            """encoding: block_simdbp
path: inv:block_simdbp:bm25:b=2.0:k1=1.0:quantization=8
quantization: 8
scorer:
  b: 2.0
  k1: 1.0
  name: bm25
""",
        ),
        (
            metadata.CompressedIndexFile(
                path=Path("/path/to/index"),
                encoding="block_simdbp",
                quantization=8,
                scorer=scorer.BM25(name="bm25", k1=1.0, b=2.0),
            ),
            """encoding: block_simdbp
path: /path/to/index
quantization: 8
scorer:
  b: 2.0
  k1: 1.0
  name: bm25
""",
        ),
    ],
)
def test_compressed_index_file(index: metadata.CompressedIndexFile, yaml: str):
    assert to_yaml_str(index, by_alias=True, exclude_none=True) == yaml
    assert parse_yaml_raw_as(metadata.CompressedIndexFile, yaml) == index


@pytest.mark.parametrize(
    "collection,yaml",
    [
        (
            metadata.CollectionMetadata(workdir=Path("/workdir")),
            """analyzer:
  strip_html: true
  token_filters:
  - lowercase
  - porter2
  tokenizer: english
workdir: /workdir
""",
        ),
        (
            metadata.CollectionMetadata(
                workdir=Path("/workdir"),
                forward_index=Path("fwd"),
                terms=Path("terms"),
                term_lexicon=Path("termlex"),
                orderings={
                    "default": metadata.DocumentOrdering(
                        documents=Path("documents"),
                        document_lexicon=Path("doclex"),
                        urls=Path("urls"),
                        uncompressed_index=metadata.UncompressedIndex(
                            documents=Path("inv.docs"),
                            values=Path("inv.freqs"),
                            sizes=Path("inv.sizes"),
                        ),
                        compressed_index_files=[
                            metadata.CompressedIndexFile(
                                path=Path("inv.simdbp"),
                                encoding="block_simdbp",
                                quantization=8,
                                scorer=scorer.BM25(name="bm25", k1=1.0, b=2.0),
                            ),
                            metadata.CompressedIndexFile(
                                path=Path("inv.pef"),
                                encoding="pef",
                            ),
                        ],
                        wand_data_files=[
                            metadata.WandData(
                                path=Path("/path/to/wdata"),
                                block=metadata.FixedBlock(size=64),
                                scorer=scorer.BM25(name="bm25", k1=1.0, b=2.0),
                            ),
                            metadata.WandData(
                                path=Path("/path/to/wdata.qld"),
                                block=metadata.FixedBlock(size=32),
                                scorer=scorer.QLD(name="qld", mu=1000.0),
                            ),
                        ],
                        compressed_indexes={
                            "simdbp": metadata.CompressedIndex(
                                encoding="block_simdbp",
                                quantization=8,
                                scorer=scorer.BM25(name="bm25", k1=1.0, b=2.0),
                                block=metadata.FixedBlock(size=64),
                            ),
                            "pef-bm25": metadata.CompressedIndex(
                                encoding="pef",
                                scorer=scorer.BM25(name="bm25", k1=1.0, b=2.0),
                                block=metadata.FixedBlock(size=64),
                            ),
                            "pef-qld": metadata.CompressedIndex(
                                encoding="pef",
                                scorer=scorer.QLD(name="qld", mu=1000.0),
                                block=metadata.FixedBlock(size=32),
                            ),
                        },
                    )
                },
            ),
            """analyzer:
  strip_html: true
  token_filters:
  - lowercase
  - porter2
  tokenizer: english
forward_index: fwd
orderings:
  default:
    compressed_index_files:
    - encoding: block_simdbp
      path: inv.simdbp
      quantization: 8
      scorer:
        b: 2.0
        k1: 1.0
        name: bm25
    - encoding: pef
      path: inv.pef
    compressed_indexes:
      pef-bm25:
        block:
          size: 64
        encoding: pef
        scorer:
          b: 2.0
          k1: 1.0
          name: bm25
      pef-qld:
        block:
          size: 32
        encoding: pef
        scorer:
          mu: 1000.0
          name: qld
      simdbp:
        block:
          size: 64
        encoding: block_simdbp
        quantization: 8
        scorer:
          b: 2.0
          k1: 1.0
          name: bm25
    document_lexicon: doclex
    documents: documents
    uncompressed_index:
      documents: inv.docs
      sizes: inv.sizes
      values: inv.freqs
    urls: urls
    wand_data_files:
    - block:
        size: 64
      path: /path/to/wdata
      scorer:
        b: 2.0
        k1: 1.0
        name: bm25
    - block:
        size: 32
      path: /path/to/wdata.qld
      scorer:
        mu: 1000.0
        name: qld
term_lexicon: termlex
terms: terms
workdir: /workdir
""",
        ),
    ],
)
def test_collection_metadata(collection: metadata.CollectionMetadata, yaml: str):
    assert to_yaml_str(collection, by_alias=True, exclude_none=True) == yaml
    assert parse_yaml_raw_as(metadata.CollectionMetadata, yaml) == collection
