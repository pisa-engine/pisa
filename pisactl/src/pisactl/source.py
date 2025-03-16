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

"""Source of data for indexing."""

import enum
from pathlib import Path

import pydantic

from pisactl.metadata import Analyzer


class ParseFormat(enum.Enum):
    JSONL = "jsonl"
    PLAINTEXT = "plaintext"
    # TODO...


class CiffSource(pydantic.BaseModel):
    """CIFF file source."""

    input: Path


class StdinSource(pydantic.BaseModel):
    """Collection is piped directly into the program."""

    analyzer: Analyzer
    format: ParseFormat


class IrDatasetsSource(pydantic.BaseModel):
    """Collection from ir-datasets."""

    analyzer: Analyzer
    name: str
    content_fields: list[str]


class VectorsSource(pydantic.BaseModel): ...


Source = CiffSource | StdinSource | IrDatasetsSource | VectorsSource
