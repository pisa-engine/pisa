Document Reordering
===================

Random Ordering
===============


Recursive Graph Bisection
============================

Recursive graph bisection algorithm used for inverted indexed reordering.


Description
-----------

Implementation of the *Recursive Graph Bisection* (aka *BP*) algorithm which is currently the state-of-the-art for minimizing the compressed space used by an inverted index (or graph) through document reordering.
The  algorithm tries to minimize an objective function directly related to the number of bits needed to storea graph or an index using a delta-encoding scheme.

>  L.  Dhulipala,  I.  Kabiljo,  B.  Karrer,  G.  Ottaviano,  S.  Pupyrev,  andA.  Shalita.   Compressing  graphs  and  indexes  with  recursive  graph  bisec-tion.  InProc. SIGKDD, pages 1535–1544, 2016.

Usage
-----

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
