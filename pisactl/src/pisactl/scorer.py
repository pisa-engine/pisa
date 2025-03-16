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

from typing import Literal
import pydantic


class BM25(pydantic.BaseModel):
    name: Literal["bm25"]
    k1: float
    b: float


class PL2(pydantic.BaseModel):
    name: Literal["pl2"]
    c: float


class QLD(pydantic.BaseModel):
    name: Literal["qld"]
    mu: float


class DPH(pydantic.BaseModel):
    name: Literal["dph"]


class Passthrough(pydantic.BaseModel):
    name: Literal["passthrough"]


Scorer = BM25 | PL2 | QLD | DPH | Passthrough


def resolve(args) -> Scorer:
    match args.scorer:
        case "bm25":
            return BM25(name="bm25", k1=0.9, b=0.4)
        case "pl2":
            return PL2(name="pl2", c=1)
        case "qld":
            return QLD(name="qld", mu=1000)
        case "dph":
            return DPH(name="dph")
        case "passthrough":
            return Passthrough(name="passthrough")
    raise ValueError(f"invalid scorer name: {args.name}")


def serialize_for_filename(scorer: Scorer):
    match scorer:
        case BM25(name=name, k1=k1, b=b):
            return f"{name}:b={b}:k1={k1}"
        case PL2(name=name, c=c):
            return f"{name}:c={c}"
        case QLD(name=name, mu=mu):
            return f"{name}:mu={mu}"
        case DPH(name=name):
            return name
        case Passthrough(name=name):
            return name


def scorer_args(scorer: Scorer) -> list[str]:
    name_arg = ["--scorer", scorer.name]
    match scorer:
        case BM25(k1=k1, b=b):
            return [*name_arg, "--bm25-b", str(b), "--bm25-k1", str(k1)]
        case PL2(c=c):
            return [*name_arg, "--pl2-c", str(c)]
        case QLD(mu=mu):
            return [*name_arg, "--qld-mu", str(mu)]
        case DPH():
            return name_arg
        case Passthrough():
            return ["--scorer", "quantized"]
