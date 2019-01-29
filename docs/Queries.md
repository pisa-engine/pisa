Query an index
==============

Usage
--------------

```
queries - a tool for performing queries on an index.
Usage: ./bin/queries [OPTIONS]

Options:
  -h,--help                   Print this help message and exit
  -t,--type TEXT REQUIRED     Index type
  -a,--algorithm TEXT REQUIRED
                              Query algorithm
  -i,--index TEXT REQUIRED    Collection basename
  -w,--wand TEXT              Wand data filename
  -q,--query TEXT             Queries filename
  --compressed-wand           Compressed wand input file
  -k UINT                     k value
```


Now it is possible to query the index. The command `queries` parses each line of the standard input as a tab-separated collection of term-ids, where the i-th
term is the i-th list in the input collection.

    $ ./bin/queries -t opt -a and -i test_collection.index.opt -w test_collection.wand -q ../test/test_data/queries

This performs conjunctive queries (`and`). In place of `and` other operators can
be used (`or`, `wand`, ..., see `queries.cpp`), and also multiple operators
separated by colon (`and:or:wand`).

If the WAND file is compressed, please append `--compressed-wand` flag.
