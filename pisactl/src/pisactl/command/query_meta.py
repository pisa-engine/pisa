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


import pathlib

from pisactl import metadata
from pydantic_yaml import to_yaml_str


def query_meta(args, ordering: str = "default"):
    meta = metadata.CollectionMetadata.load(pathlib.Path(args.workdir))
    assert meta.orderings is not None
    ordering_meta = meta.orderings[ordering]
    match args.subcommand:
        case "print":
            print(to_yaml_str(meta, exclude_none=True).strip())
        case "aliases":
            for name in (ordering_meta.compressed_indexes or {}).keys():
                print(name)
        case "alias":
            index_meta = ordering_meta.get_by_alias(args.alias)
            print(to_yaml_str(index_meta, exclude_none=True).strip())
        case _:
            ValueError("unknown subcommand")
