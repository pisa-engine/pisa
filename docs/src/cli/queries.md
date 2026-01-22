# queries

## Usage

```
<!-- cmdrun ../../../build/bin/queries --help -->
```

## Description

Runs query benchmarks focused on performance measurement, executing each query
on the given index multiple times. Then, it aggregates statistics across all
queries.

Note: for retrieval results use `evaluate_queries`.

## Input

This program takes a compressed index as its input along with a file
containing the queries (line by line). Note that you need to specify the
correct index encoding with `--encoding` option, as this is currently
not stored in the index. If the index is quantized, you must pass
`--quantized` flag.

For certain types of retrieval algorithms, you will also need to pass
the so-called "WAND file", which contains some metadata like skip lists
and max scores.

## Query Parsing

There are several parameters you can define to instruct the program on
how to parse and process the input queries, including which tokenizer to
use, whether to strip HTML from the query, and a list of token filters
(such as stemmers). For a more comprehensive description, see
[`parse_collection`](parse_collection.html).

You can also pass a file containing stop-words, which will be excluded
from the parsed queries.

In order for the parsing to actually take place, you need to also
provide the term lexicon with `--terms`. If not defined, the queries
will be interpreted as lists of document IDs.

## Algorithm

You can specify what retrieval algorithm to use with `--algorithm`.
Furthermore, `-k` option defined how many results to retrieve for each
query.

## Scoring

Use `--scorer` option to define which scoring function you want to use
(`bm25`, `dph`, `pl2`, `qld`). Some scoring functions have additional
parameters that you may override, see the help message above.

## Thresholds

You can also pass a file with list of initial score thresholds. Any
documents that evaluate to a score below this value will be excluded.
This can speed up the algorithm, but if the threshold is too high, it
may exclude some of the relevant top-k results. If you want to always
ensure that the results are as if the initial threshold was zero, you
can pass `--safe` flag. It will force to recompute the entire query
_without an initial threshold_ if it is detected that relevant documents
have been excluded. This may be useful if you have mostly accurate
threshold estimates, but still need the safety: even though some queries
will be slower, most will be much faster, thus improving overall
throughput and average latency.
