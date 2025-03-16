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
import subprocess
import sys

from pisactl import metadata
from pisactl.scorer import BM25, PL2, QLD, Scorer, scorer_args


class ToolError(Exception):
    def __init__(self, cmd: str):
        self.cmd = cmd

    def __str__(self) -> str:
        return f"ERROR: command failed: {self.cmd}"


class Tools:
    def __init__(self, bin_dir: str | None, verbose: bool = False) -> None:
        self.bin_dir = pathlib.Path(bin_dir) if bin_dir else None
        self.verbose = verbose

    def _command(self, name: str) -> str:
        if self.bin_dir is None:
            return name
        return str(self.bin_dir / name)

    def _cmd_ir_datasets(self) -> str:
        return self._command("ir-datasets")

    def _cmd_parse_collection(self) -> str:
        return self._command("parse_collection")

    def _cmd_invert(self) -> str:
        return self._command("invert")

    def _cmd_compress_inverted_index(self) -> str:
        return self._command("compress_inverted_index")

    def _cmd_create_wand_data(self) -> str:
        return self._command("create_wand_data")

    def _cmd_evaluate_queries(self) -> str:
        return self._command("evaluate_queries")

    def _cmd_queries(self) -> str:
        return self._command("queries")

    def _cmd_lexicon(self) -> str:
        return self._command("lexicon")

    def _cmd_ciff_to_pisa(self) -> str:
        return self._command("ciff2pisa")

    def _count_terms(self, meta: metadata.CollectionMetadata) -> int:
        if meta.terms is None:
            raise ValueError("cannot count terms because terms file does not exist")
        with open(meta.workdir / meta.terms, "r") as f:
            return sum(1 for line in f if line.strip() != "")

    def _run(self, args: list[str]):
        try:
            if self.verbose:
                print("#", " ".join(args), file=sys.stderr)
            subprocess.run(args, check=True)
        except subprocess.CalledProcessError:
            raise ToolError(" ".join(args))

    def ciff_to_pisa(
        self, ciff_file: str, output_dir: str, *, force: bool
    ) -> metadata.CollectionMetadata:
        workdir = pathlib.Path(output_dir)
        workdir.mkdir(exist_ok=force)
        self._run(
            [self._cmd_ciff_to_pisa(), "--ciff-file", ciff_file, "--output", str(workdir / "ciff")]
        )
        self._run(
            [
                self._cmd_lexicon(),
                "build",
                str(workdir / "ciff.terms"),
                str(workdir / "ciff.termlex"),
            ]
        )
        self._run(
            [
                self._cmd_lexicon(),
                "build",
                str(workdir / "ciff.documents"),
                str(workdir / "ciff.doclex"),
            ]
        )
        return metadata.CollectionMetadata(
            workdir=workdir,
            terms=pathlib.Path("ciff.terms"),
            term_lexicon=pathlib.Path("ciff.termlex"),
            orderings={
                "default": metadata.DocumentOrdering(
                    documents=pathlib.Path("ciff.documents"),
                    document_lexicon=pathlib.Path("ciff.doclex"),
                    uncompressed_index=metadata.UncompressedIndex(
                        documents=pathlib.Path("ciff.docs"),
                        values=pathlib.Path("ciff.freqs"),
                        sizes=pathlib.Path("ciff.sizes"),
                    ),
                )
            },
        )

    def _parse_pipe(
        self, output_dir: str, pipe, *, analyzer: metadata.Analyzer, fmt: str, force: bool
    ) -> metadata.CollectionMetadata:
        """Parse a dataset from the collection piped into stdin."""

        workdir = pathlib.Path(output_dir)

        meta = metadata.CollectionMetadata(
            analyzer=analyzer,
            workdir=workdir,
            forward_index=pathlib.Path("fwd"),
            terms=pathlib.Path("terms"),
            term_lexicon=pathlib.Path("termlex"),
            orderings={
                "default": metadata.DocumentOrdering(
                    documents=pathlib.Path("documents"),
                    document_lexicon=pathlib.Path("doclex"),
                    urls=pathlib.Path("urls"),
                )
            },
        )

        workdir.mkdir(exist_ok=force)
        args = [
            self._cmd_parse_collection(),
            "--format",
            fmt,
            "--output",
            str(workdir / "fwd"),
            "--tokenizer",
            meta.analyzer.tokenizer,
        ]

        if meta.analyzer.strip_html:
            args.append("--html")
        for token_filter in meta.analyzer.token_filters:
            args += ["-F", token_filter]

        subprocess.run(args, stdin=pipe)

        for name in ["documents", "terms", "urls", "doclex", "termlex"]:
            (workdir / f"fwd.{name}").rename(workdir / name)

        return meta

    def parse_pipe(
        self, output_dir: str, *, analyzer: metadata.Analyzer, fmt: str, force: bool
    ) -> metadata.CollectionMetadata:
        """Parse a dataset from the collection piped into stdin."""

        return self._parse_pipe(output_dir, sys.stdin, analyzer=analyzer, fmt=fmt, force=force)

    def parse_ir_datasets(
        self,
        name: str,
        output_dir: str,
        *,
        analyzer: metadata.Analyzer,
        content_fields: list[str],
        force: bool,
    ) -> metadata.CollectionMetadata:
        """Parse a dataset from `ir-datasets`, producing a forward index with supporting files,
        such as term and document lexicons."""

        ir = subprocess.Popen(
            [self._cmd_ir_datasets(), name, "--content-fields", *content_fields],
            stdout=subprocess.PIPE,
        )
        return self._parse_pipe(output_dir, ir.stdout, analyzer=analyzer, fmt="jsonl", force=force)

    def invert_forward_index(
        self, meta: metadata.CollectionMetadata, *, ordering: str = "default"
    ) -> metadata.CollectionMetadata:
        assert meta.forward_index is not None
        term_count = self._count_terms(meta)
        subprocess.run(
            [
                self._cmd_invert(),
                "--input",
                str(meta.workdir / meta.forward_index),
                "--output",
                str(meta.workdir / "inv"),
                "--term-count",
                str(term_count),
            ],
            check=True,
        )
        if meta.orderings is None:
            meta.orderings = {}
        meta.orderings[ordering].uncompressed_index = metadata.UncompressedIndex(
            documents=pathlib.Path("inv.docs"),
            values=pathlib.Path("inv.freqs"),
            sizes=pathlib.Path("inv.sizes"),
        )
        return meta

    def _compress(
        self,
        workdir: pathlib.Path,
        input_base: str,
        params: metadata.CompressedIndexFile,
        *,
        wdata: metadata.WandData,
    ):
        args = [
            self._cmd_compress_inverted_index(),
            "--collection",
            input_base,
            "--output",
            str(workdir / params.path),
            "--check",
            "--encoding",
            params.encoding,
        ]
        if params.quantization is not None:
            assert params.scorer is not None
            args += [
                "--wand",
                str(workdir / wdata.path),
                "--quantize",
                str(params.quantization),
                "--scorer",
                params.scorer.name,
            ]
            match params.scorer:
                case BM25(b=b, k1=k1):
                    args += ["--bm25-b", str(b), "--bm25-k1", str(k1)]
                case QLD(mu=mu):
                    args += ["--qld-mu", str(mu)]
                case PL2(c=c):
                    args += ["--pl2-c", str(c)]
        subprocess.run(args, check=True)

    def _create_wdata(self, workdir: pathlib.Path, input_base: str, wdata: metadata.WandData):
        args = [
            self._cmd_create_wand_data(),
            "--collection",
            input_base,
            "--output",
            str(workdir / wdata.path),
            *scorer_args(wdata.scorer),
        ]
        match wdata.block:
            case metadata.FixedBlock(size=size):
                args += ["--block-size", str(size)]
            case metadata.VariableBlock(lambda_=lambda_):
                args += ["--lambda", str(lambda_)]
        subprocess.run(args, check=True)

    def compress(
        self,
        meta: metadata.CollectionMetadata,
        encoding: str,
        *,
        scorer: Scorer,
        block: metadata.FixedBlock | metadata.VariableBlock,
        quantization: int | None = None,
        alias: str = "default",
        ordering: str = "default",
    ) -> metadata.CollectionMetadata:
        """Compress and create wand data structure.

        It will only _build_ another structure if it is needed. For example, imagine we are testing
        retrieval algorithms with multiple index encodings _without_ quantization. In such case,
        the index structure itself does not depend on the scorer. This means that we can build one
        wand data structure that is shared across all indexes. The first time we call this function,
        it will build both wand data and index, but if we call it the second time with another
        encoding, it will locate the already existing wand data and link it to the new index.

        Similarly, if we want to test a single index encoding with different scorers or block sizes,
        the index does not have to be rebuilt.

        Of course, if the index is _quantized_, it is tightly coupled to the scorer and thus limits
        the ability to reuse wand data. However, even then, we could reuse the index if, say, we
        are testing multiple block size parameters.
        """

        assert meta.orderings is not None

        ordering_meta = meta.orderings[ordering]
        assert ordering_meta.uncompressed_index is not None

        inv_base = str(meta.workdir / ordering_meta.uncompressed_index.documents).removesuffix(
            ".docs"
        )

        idx_meta = metadata.CompressedIndex(
            encoding=encoding, scorer=scorer, block=block, quantization=quantization
        )
        ordering_meta.add_compressed_index(alias, idx_meta)

        wdata = ordering_meta.resolve_wdata(idx_meta)
        if wdata is None:
            wdata = metadata.WandData(block=block, scorer=scorer, quantization=quantization)
            self._create_wdata(meta.workdir, inv_base, wdata)
            ordering_meta.add_wand_data_file(wdata)

        compressed = ordering_meta.resolve_compressed(idx_meta)
        if compressed is None:
            compressed_index_file = metadata.CompressedIndexFile(
                encoding=encoding,
                # if not quantized, it does not depend on the scorer
                scorer=scorer if quantization is not None else None,
                quantization=quantization,
            )
            self._compress(meta.workdir, inv_base, compressed_index_file, wdata=wdata)
            ordering_meta.add_compressed_index_file(compressed_index_file)

        return meta

    def queries(
        self,
        meta: metadata.CollectionMetadata,
        alias: str,
        input_file: pathlib.Path,
        *,
        k: int,
        algorithm: str,
        benchmark: bool = False,
        ordering: str = "default",
    ):
        assert meta.orderings is not None
        ordering_meta = meta.orderings[ordering]
        cmd = self._cmd_queries() if benchmark else self._cmd_evaluate_queries()

        index = ordering_meta.get_by_alias(alias)

        compressed_index = ordering_meta.resolve_compressed(index)
        if compressed_index is None:
            raise metadata.IndexResolutionError(alias, index)

        wdata = ordering_meta.resolve_wdata(index)
        if wdata is None:
            raise metadata.WandDataResolutionError(alias, index)

        index_args = [
            cmd,
            "--encoding",
            index.encoding,
            "--index",
            str(meta.workdir / compressed_index.path),
            "--wand",
            str(meta.workdir / wdata.path),
        ]

        assert index.scorer is not None, "scorer must be defined"
        assert ordering_meta.document_lexicon is not None, "document lexicon must be defined"
        assert meta.term_lexicon is not None, "term lexicon must be defined"

        docargs = []
        if not benchmark:
            docargs += ["--documents", str(meta.workdir / ordering_meta.document_lexicon)]

        self._run(
            [
                *index_args,
                *scorer_args(index.scorer),
                "--terms",
                str(meta.workdir / meta.term_lexicon),
                *docargs,
                "-k",
                str(k),
                "--algorithm",
                algorithm,
                "--queries",
                str(input_file),
            ],
        )
