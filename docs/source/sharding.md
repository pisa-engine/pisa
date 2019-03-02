Sharding
========

We support partitioning a collection into a number of smaller subsets called _shards_.
Right now, only a forward index can be partitioned by running `partition_fwd_index` command.
Then, the resulting shards must be inverted individually with `invert` command.
For convenience, we provide `script/invert-shards` that takes a file prefix
to shard forward indexes and inverts them all.

## `partition_fwd_index`

    Partition a forward index
    Usage: ./bin/partition_fwd_index [OPTIONS]
    
    Options:
      -h,--help                   Print this help message and exit
      -i,--input TEXT REQUIRED    Forward index filename
      -o,--output TEXT REQUIRED   Basename of partitioned shards
      -j,--threads INT            Thread count
      -r,--random-shards INT Excludes: --shard-files
                                  Number of random shards
      -s,--shard-files TEXT ... Excludes: --random-shards
                                  List of files with shard titles
      --debug                     Print debug messages

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

## `invert-shards.sh`

This script inverts all shards with a common prefix.

    USAGE:
        invert-shards <PROGRAM> <INPUT_BASENAME> <OUTPUT_BASENAME> [program flags] 

For example, if the following command was used to partition a collection:

    $ partition_fwd_index \
        -j 8 \                          # use up to 8 threads at a time
        -i full_index_prefix \
        -o shard_prefix \
        -r 123                          # partition randomly into 123 shards

Then, one can invert the shards by executing the following script:

    $ invert-shards.sh \
        /path/to/invert \               # provide path to program
        shard_prefix \                  # basename to shard collections
        shard_prefix_inverted \         # basename to shard inverted indexes
        -j 8 -b 1000                    # any arguments to be appended to each program execution

## `compress-shards.sh`

Next, you can compress the inverted shards with `compress-shards.sh`:

    USAGE:
        compress-shards <PROGRAM> <INPUT_BASENAME> <OUTPUT_BASENAME> [program flags] 

For example, following the above example:

    $ compress-shards.sh \
        /path/to/create_freq_index \    # provide path to program
        shard_prefix_inverted \         # basename to shard inverted indexes
        shard_prefix_inverted_simdbp \  # basename to shard compressed indexes
        -t block_simdbp --check         # any arguments to be appended to each program execution
