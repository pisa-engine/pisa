# Document Reordering

## Document name & URL based ordering
To reorder an inverted index based on document names or their URLs, use the `generate_sorted_docids_mapping.py` script and the `shuffle_docids` command.

The first one allows to generate a file in which each line maps a `<current ID>` with a `<new ID>`. These identifiers are generated based on the lexicographical order of  document names (or their URLs). Furthermore, this file serves as input to the `shuffle_docids` command, which materializes the reordering operation.

### 1. Mapping file creation
To generate the mapping file, it is necessary to take into account that each line number of the `.documents` and `.urls` files (which are part of the forward index) correspond to its docid. So, the first document of `.documents` (line 0, and therefore docid = 0) is equivalent to the first URL in the `.urls` file, and so on. In this way, if you want to generate the mapping file to perform a reordering using either files, you should use the following script:

```
usage: script/generate_sorted_docids_mapping.py [-h] documents output

Take a text file lexicon (e.g. '.documents' or '.urls' file) and sort it,
generating a file mapping '<current ID> <new ID>' to use with the
'suffle_docids' script.

positional arguments:
  documents   File containing one document (or URL) per line and where each
              line number (starting from zero) represents its docid
  output      Output file mapping '<current ID> <new ID>'

optional arguments:
  -h, --help  show this help message and exit
```

For example:
```
$ python script/generate_docids_sorted_map.py path/to/inverted.urls mapping.txt
```
Note that in the example the `.urls` file is used for reordering. If you want to generate the mapping based on document names, the `.documents` file should be used instead.

### 2. Index remapping
Once the mapping file has been created, you must use the `shuffle_docids` command to generate the new inverted index:
```
Usage: ./bin/shuffle_docids <collection basename> <output basename> [ordering file]
Ordering file is of the form <current ID> <new ID>
```

For example:
```
$ mkdir -p path/to/ordered
$ ./bin/shuffle_docids path/to/inverted path/to/ordered/inverted mapping.txt
```

## Random Ordering

If you want to perfom a random ordering, use the `shuffle_docids` command (described in the previous section), but without specifying the `[ordering file]` option.

## Recursive Graph Bisection


Recursive graph bisection algorithm used for inverted indexed reordering.


### Description

Implementation of the *Recursive Graph Bisection* (aka *BP*) algorithm which is currently the state-of-the-art for minimizing the compressed space used by an inverted index (or graph) through document reordering.
The  algorithm tries to minimize an objective function directly related to the number of bits needed to storea graph or an index using a delta-encoding scheme.

>  L.  Dhulipala,  I.  Kabiljo,  B.  Karrer,  G.  Ottaviano,  S.  Pupyrev,  andA.  Shalita.   Compressing  graphs  and  indexes  with  recursive  graph  bisec-tion.  InProc. SIGKDD, pages 1535–1544, 2016.

### Usage

```
Recursive graph bisection algorithm used for inverted indexed reordering.
Usage: ./bin/recursive_graph_bisection [OPTIONS]

Options:
  -h,--help                   Print this help message and exit
  -c,--collection TEXT REQUIRED
                              Collection basename
  -o,--output TEXT            Output basename
  --store-fwdidx TEXT         Output basename (forward index)
  --fwdidx TEXT               Use this forward index
  -m,--min-len UINT           Minimum list threshold
  -d,--depth INT in [1 - 64] Excludes: --config
                              Recursion depth
  -t,--threads UINT           Thread count
  --prelim UINT               Precomputing limit
  --config TEXT Excludes: --depth
                              Node configuration file
  --nogb                      No VarIntGB compression in forward index
  -p,--print                  Print ordering to standard output

```
