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
import tempfile

from ruamel.yaml import sys

from pisactl import metadata
from pisactl.tools import Tools


def query(tools: Tools, args):
    meta = metadata.CollectionMetadata.load(pathlib.Path(args.workdir))
    with tempfile.NamedTemporaryFile("w", delete_on_close=False) as input_file:
        queries = sys.stdin.readlines()
        input_file.writelines(queries)
        input_file.close()
        tools.queries(
            meta,
            args.alias,
            pathlib.Path(input_file.name),
            k=args.k,
            algorithm=args.algorithm,
            benchmark=args.benchmark,
        )
