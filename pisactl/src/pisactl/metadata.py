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

import pydantic
from pydantic_yaml import parse_yaml_file_as, to_yaml_file

from pisactl.scorer import Scorer, serialize_for_filename


class FixedBlock(pydantic.BaseModel):
    size: int


class VariableBlock(pydantic.BaseModel):
    lambda_: float = pydantic.Field(alias="lambda")


Block = FixedBlock | VariableBlock


class UncompressedIndex(pydantic.BaseModel):
    """Paths to files comprising the uncompressed index."""

    documents: Path
    values: Path
    sizes: Path


class WandData(pydantic.BaseModel):
    path: Path = Path("")  # by default it will be generated from other fields
    block: FixedBlock | VariableBlock
    quantization: int | None = None
    scorer: Scorer = pydantic.Field(discriminator="name")

    @pydantic.model_validator(mode="after")
    def check_passwords_match(self) -> "WandData":
        if self.path == Path(""):
            self.path = Path(
                f"wdata:{self.block}:{serialize_for_filename(self.scorer)}"
                + (f":quantization={self.quantization}" if self.quantization is not None else "")
            )
        return self


class CompressedIndexFile(pydantic.BaseModel):
    path: Path = Path("")  # by default it will be generated from other fields
    encoding: str
    quantization: int | None = None
    scorer: Scorer | None = pydantic.Field(default=None, discriminator="name")

    @pydantic.model_validator(mode="after")
    def check_passwords_match(self) -> "CompressedIndexFile":
        if self.path == Path(""):
            self.path = Path(
                f"inv:{self.encoding}"
                + (f":{serialize_for_filename(self.scorer)}" if self.scorer is not None else "")
                + (f":quantization={self.quantization}" if self.quantization is not None else "")
            )
        return self


class CompressedIndex(pydantic.BaseModel):
    encoding: str
    quantization: int | None = None
    scorer: Scorer | None = pydantic.Field(default=None, discriminator="name")
    block: FixedBlock | VariableBlock | None


class Analyzer(pydantic.BaseModel):
    tokenizer: str = "english"
    strip_html: bool = True
    token_filters: list[str] = pydantic.Field(default_factory=lambda: ["lowercase", "porter2"])


class DocumentOrdering(pydantic.BaseModel):
    documents: Path | None = None
    document_lexicon: Path | None = None
    urls: Path | None = None
    uncompressed_index: UncompressedIndex | None = None
    compressed_indexes: dict[str, CompressedIndex] | None = None
    compressed_index_files: list[CompressedIndexFile] | None = None
    wand_data_files: list[WandData] | None = None

    def resolve_compressed(self, index: CompressedIndex) -> CompressedIndexFile | None:
        """Find compressed index file matching metadata or None."""
        for compressed in self.compressed_index_files or []:
            if compressed.encoding != index.encoding:
                continue
            if compressed.quantization != index.quantization:
                continue
            if compressed.quantization is None and compressed.scorer is None:
                return compressed
            if compressed.quantization is not None and compressed.scorer == index.scorer:
                return compressed
        return None

    def resolve_wdata(self, index: CompressedIndex) -> WandData | None:
        """Find wand data matching metadata or None."""
        for wdata in self.wand_data_files or []:
            if (
                wdata.block == index.block
                and wdata.quantization == index.quantization
                and wdata.scorer == index.scorer
            ):
                return wdata
        return None

    def add_compressed_index_file(self, meta: CompressedIndexFile):
        if self.compressed_index_files is None:
            self.compressed_index_files = []
        self.compressed_index_files.append(meta)

    def add_wand_data_file(self, meta: WandData):
        if self.wand_data_files is None:
            self.wand_data_files = []
        self.wand_data_files.append(meta)

    def add_compressed_index(self, alias: str, meta: CompressedIndex):
        if self.compressed_indexes is None:
            self.compressed_indexes = {}
        if alias in self.compressed_indexes:
            raise ValueError(f"alias already exists: {alias}")
        self.compressed_indexes[alias] = meta

    def get_by_alias(self, alias: str) -> CompressedIndex:
        index_meta = (self.compressed_indexes or {}).get(alias)
        if index_meta is None:
            raise AliasNotFound(alias)
        return index_meta


class CollectionMetadata(pydantic.BaseModel):
    """Metadata for a fully indexed collection.

    All paths are relative to `workdir`.
    """

    workdir: Path
    analyzer: Analyzer = Analyzer()
    forward_index: Path | None = None
    terms: Path | None = None
    term_lexicon: Path | None = None
    orderings: dict[str, DocumentOrdering] | None = None

    def dump(self) -> None:
        with open(self.workdir / "metadata.yaml", "w", encoding="utf-8") as f:
            to_yaml_file(f, self, by_alias=True, exclude_none=True)

    @staticmethod
    def load(workdir: Path) -> "CollectionMetadata":
        try:
            with open(workdir / "metadata.yaml", "r", encoding="utf-8") as f:
                return parse_yaml_file_as(CollectionMetadata, f)
        except FileNotFoundError:
            raise MetadataNotFound(workdir)


class IndexResolutionError(Exception):
    def __init__(self, alias: str, index_meta: CompressedIndex):
        self.alias = alias
        self.index_meta = index_meta

    def __str__(self) -> str:
        return f"could not resolve compressed index file for alias '{self.alias}' with {self.index_meta}"


class WandDataResolutionError(Exception):
    def __init__(self, alias: str, index_meta: CompressedIndex):
        self.alias = alias
        self.index_meta = index_meta

    def __str__(self) -> str:
        return f"could not resolve wand data file for alias '{self.alias}' with {self.index_meta}"


class MetadataNotFound(Exception):
    def __init__(self, path: str | Path):
        self.path = path

    def __str__(self) -> str:
        return f"could not locate metadata file: {self.path}"


class AliasNotFound(Exception):
    def __init__(self, alias: str):
        self.alias = alias

    def __str__(self) -> str:
        return f"alias does not exist: {self.alias}"
