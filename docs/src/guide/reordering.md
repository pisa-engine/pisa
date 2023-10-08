# Document Reordering

PISA supports reassigning document IDs that were initially assigned in order of parsing.
The point of doing it is usually to decrease the index size or speed up query processing.
This part is done on an uncompressed inverted index.
Depending on the method, you might also need access to some parts of the forward index.
We support the following ways of reordering:
- random,
- by a feature (such as URL or document TREC ID),
- with a custom-defined mapping, and
- recursive graph bisection.
All of the above are supported by a single command `reorder-docids`.
Below, we explain each method and show some examples of running the command.

## Reordering document lexicon

All methods can optionally take a path to a document lexicon and make a copy of it that reflects
the produced reordering.

```bash
reorder-docids \
    --documents /path/to/original/doclex \
    --reordered-documents /path/to/reordered/doclex \
    ...
```

Typically, you will want to do that if you plan to evaluate queries, which will need access to
a correct document lexicon.

> **NOTE**: Because these options are common to all reordering methods, we ignore them below for brevity.

## Random

Random document reordering, as the name suggests, randomly shuffles all document IDs.
Additionally, it can take a random seed. Two executions of the command with the same seed
will produce the same final ordering.

```bash
reorder-docids --random \
    --collection /path/to/inv \
    --output /path/to/inv.random \
    --seed 123456789 # optional
```

## By feature (e.g., URL or TRECID)

An index can be reordered according to any single document feature, such as URL or TRECID,
as long as it is stored in a text file line by line, where line `n` is the feature of
document `n` in the original order.

In particular, our collection parsing command produces two such feature files:
- `*.documents`, which is typically a list of TRECIDs,
- `*.urls`, which is a list of document URLs.

To use either, you simply need to run:

```bash
reorder-docids \
    --collection /path/to/inv \
    --output /path/to/inv.random \
    --by-feature /path/to/feature/file
```

## From custom mapping

You can also produce a mapping yourself and feed it to the command.
Such mapping is a text file with two columns separated by a whitespace:

```
<original ID> <new ID>
```

Having that, reordering is as simple as running:

```bash
reorder-docids \
    --collection /path/to/inv \
    --output /path/to/inv.random \
    --from-mapping /path/to/custom/mapping
```

## Recursive Graph Bisection

We provide an implementation of the *Recursive Graph Bisection* (aka *BP*) algorithm,
which is currently the state-of-the-art for minimizing the compressed space used
by an inverted index (or graph) through document reordering.
The algorithm tries to minimize an objective function directly related to the number
of bits needed to store a graph or an index using a delta-encoding scheme.

Learn more from the original paper:

> L. Dhulipala, I. Kabiljo, B. Karrer, G. Ottaviano, S. Pupyrev, and A. Shalita.
> Compressing  graphs  and  indexes  with  recursive  graph  bisection.
> In Proc. SIGKDD, pages 1535–1544, 2016.

In PISA, you simply need to pass `--recursive-graph-bisection` option (or its alias `--bp`)
to the `reorder-docids` command.

```bash
reorder-docids --bp \
    --collection /path/to/inv \
    --output /path/to/inv.random
```

Note that `--bp` allows for some additional options.
For example, the algorithm constructs a forward index in memory, which is in a special format
**separate from the PISA forward index** that you obtain from the `parse_collection` tool.
You can instruct `reorder-docids` to store that intermediate structure (`--store-fwdidx`),
as well as provide a previously constructed one (`--fwdidx`), which can be useful if you
want to reuse it for several runs with different algorithm parameters.
To see all available parameters, run `reorder-docids --help`.
