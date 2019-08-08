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

## Inverted index format

A _binary sequence_ is a sequence of integers prefixed by its length, where both the sequence integers and the length are written as 32-bit little-endian unsigned integers. An _inverted index_ consists of 3 files, `<basename>.docs`, `<basename>.freqs`, `<basename>.sizes`:

* `<basename>.docs` starts with a singleton binary sequence where its only
  integer is the number of documents in the collection. It is then followed by
  one binary sequence for each posting list, in order of term-ids. Each posting
  list contains the sequence of document-ids containing the term.

* `<basename>.freqs` is composed of a one binary sequence per posting list, where
  each sequence contains the occurrence counts of the postings, aligned with the
  previous file (note however that this file does not have an additional
  singleton list at its beginning).

* `<basename>.sizes` is composed of a single binary sequence whose length is the
  same as the number of documents in the collection, and the i-th element of the
  sequence is the size (number of terms) of the i-th document.


### Reading the inverted index using Python

```python
import os
import numpy as np

class InvertedIndex:
    def __init__(self, index_name):
        index_dir = os.path.join(index_name)
        self.docs = np.memmap(index_name + ".docs", dtype=np.uint32,
              mode='r')
        self.freqs = np.memmap(index_name + ".freqs", dtype=np.uint32,
              mode='r')

    def __iter__(self):
        i = 2
        while i < len(self.docs):
            size = self.docs[i]
            yield (self.docs[i+1:size+i+1], self.freqs[i-1:size+i-1])
            i += size+1

    def __next__(self):
        return self

for i, (docs, freqs) in enumerate(InvertedIndex("cw09b")):
    print(i, docs, freqs)
```