Inverting
=========

Once the parsing phase is complete, use the `invert` command to turn a _forward index_ into an _inverted index_:

    invert - turn forward index into inverted index
    Usage: ./invert [OPTIONS]
    
    Options:
      -h,--help                   Print this help message and exit
      -i,--input TEXT REQUIRED    Forward index filename
      -o,--output TEXT REQUIRED   Output inverted index basename
      -j,--threads UINT           Thread count
      --term-count UINT REQUIRED  Term count
      -b,--batch-size INT=100000  Number of documents to process at a time

For example, assuming the existence of a forward index in the path `path/to/forward/cw09b`:

    $ mkdir -p path/to/inverted
    $ ./invert -i path/to/forward/cw09b -o path/to/inverted/cw09b --term-count `wc -w < path/to/forward/cw09b.terms`
    
Note that the script requires as parameter the number of terms to be indexed, which is obtained by embedding the
`wc -w < path/to/forward/cw09b.terms` instruction.
