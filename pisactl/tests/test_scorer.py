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


from unittest.mock import MagicMock

import pytest
from pisactl import scorer


def test_resolve_scorer():
    with pytest.raises(ValueError):
        scorer.resolve(MagicMock())
    assert scorer.resolve(MagicMock(scorer="bm25")) == scorer.BM25(name="bm25", k1=0.9, b=0.4)
