Sharding
========

We support partitioning a collection into a number of smaller subsets called _shards_.
Right now, only a forward index can be partitioned by running `partition_fwd_index` command.
For convenience, we provide `shards` command that supports certain bulk operations on all shards.

## Partitioning collection

We support two methods of partitioning: random, and by a defined mapping.
For example, one can partition collection randomly:

    $ partition_fwd_index \
        -j 8 \                          # use up to 8 threads at a time
        -i full_index_prefix \
        -o shard_prefix \
        -r 123                          # partition randomly into 123 shards

Alternatively, a set of files can be provided.
Let's assume we have a folder `shard-titles` with a set of text files.
Each file contains new-line-delimited document titles (e.g., TREC-IDs) for one partition.
Then, one would call:

    $ partition_fwd_index \
        -j 8 \                          # use up to 8 threads at a time
        -i full_index_prefix \
        -o shard_prefix \
        -s shard-titles/*

Note that the names of the files passed with `-s` will be ignored.
Instead, each shard will be assigned a numerical ID from `0` to `N - 1` in order
in which they are passed in the command line.
Then, each resulting forward index will have appended `.ID` to its name prefix:
`shard_prefix.000`, `shard_prefix.001`, and so on.

## Working with shards

The `shards` tool allows to perform some index operations in bulk on all shards at once.
At the moment, the following subcommands are supported:
- invert,
- compress,
- wand-data, and
- reorder-docids.

All input and output paths passed to the subcommands will be expanded for each individual shards
by extending it with `.<shard-id>` (e.g., `.000`) or, if substring `{}` is present, then
the shard number will be substituted there. For example:

```bash
shards reorder-docids --by-url \
    -c inv \
    -o inv.url \
    --documents fwd.{}.doclex \
    --reordered-documents fwd.url.{}.doclex
```

is equivalent to running the following command for every shard `XYZ`:

```bash
reorder-docids --by-url \
    -c inv.XYZ \
    -o inv.url.XYZ \
    --documents fwd.XYZ.doclex \
    --reordered-documents fwd.url.XYZ.doclex
```
