Indexes format
==============

A _binary sequence_ is a sequence of integers prefixed by its length, where both
the sequence integers and the length are written as 32-bit little-endian
unsigned integers.

A _collection_ consists of 3 files, `<basename>.docs`, `<basename>.freqs`,
`<basename>.sizes`.

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


### Reading the collection using Python

```python
import numpy as np

class Collection:
    def __init__(self, collection_name):
        self.docs = np.fromfile(open(collection_name + '.docs', "r"), dtype=np.uint32)
        self.freqs = np.fromfile(open(collection_name + ".freqs", "r"), dtype=np.uint32)

    def __iter__(self):
        i = 2
        while i < len(docs):
            size = self.docs[i]
            yield (self.docs[i+1:size+i+1], self.freqs[i+1:size+i+1])
            i+=size+1
   
    def __next__(self):
        return self

for idx, a in enumerate(Collection('test_collection')):
    print(idx, a)
```
