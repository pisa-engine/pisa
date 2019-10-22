# Compress Index

## Usage
To create an index use the command `create_freq_index`. The available index
types are listed in `index_types.hpp`. 

    create_freq_index - a tool for creating an index.
    Usage:
      create_freq_index [OPTION...]

      -h, --help                 Print help
      -t, --type type_name       Index type
      -c, --collection basename  Collection basename
      -o, --out filename         Output filename
          --check                Check the correctness of the index (default:
                                 false) 

For example, to create an index using the
optimal partitioning algorithm using the test collection, execute the command:

    $ ./bin/create_freq_index -t opt -c ../test/test_data/test_collection -o test_collection.index.opt --check

where `test/test_data/test_collection` is the _basename_ of the collection, that
is the name without the `.{docs,freqs,sizes}` extensions, and
`test_collection.index.opt` is the filename of the output index. `--check`
perform a verification step to check the correctness of the index.

## Compression Algorithms

### Binary Interpolative Coding

Binary Interpolative Coding (BIC) directly encodes a monotonically increasing sequence. At each step of this recursive algorithm, the middle element *m* is encoded by a number *m − l − p*, where *l* is the lowest value and *p* is the position of *m* in the currently encoded sequence. Then we recursively encode the values to the left and right of *m*. BIC encodings are very space-efficient, particularly on
clustered data; however, decoding is relatively slow.

To compress an index using BIC use the index type `block_interpolative`.

> Alistair Moffat, Lang Stuiver: Binary Interpolative Coding for Effective Index Compression. Inf. Retr. 3(1): 25-47 (2000)

### Elias-Fano

Given a monotonically increasing integer sequence *S* of size *n*, such that \\(S_{n-1} < u\\), we can encode it in binary using \\(\lceil\log u\rceil\\) bits.
Elias-Fano coding splits each number into two parts, a low part consisting of \\(l = \lceil\log \frac{u}{n}\rceil\\) right-most bits, and a high part consisting of the remaining \\(\lceil\log u\rceil - l\\) left-most bits. The low parts are explicitly written in binary for all numbers, in a single stream of bits. The high parts are compressed by writing, in negative-unary form, the gaps between the high parts of consecutive numbers.

To compress an index using Elias-Fano use the index type `ef`.

> Sebastiano Vigna. 2013. Quasi-succinct indices. In Proceedings of the sixth ACM international conference on Web search and data mining (WSDM ‘13). ACM, New York, NY, USA, 83-92.

### MaskedVByte

> Jeff Plaisance, Nathan Kurz, Daniel Lemire, Vectorized VByte Decoding, International Symposium on Web Algorithms 2015, 2015.

### OptPFD

> Hao Yan, Shuai Ding, and Torsten Suel. 2009. Inverted index compression and query processing with optimized document ordering. In Proceedings of the 18th international conference on World wide web (WWW '09). ACM, New York, NY, USA, 401-410. DOI: https://doi.org/10.1145/1526709.1526764

### Partitioned Elias Fano

> Giuseppe Ottaviano and Rossano Venturini. 2014. Partitioned Elias-Fano indexes. In Proceedings of the 37th international ACM SIGIR conference on Research & development in information retrieval (SIGIR '14). ACM, New York, NY, USA, 273-282. DOI: https://doi.org/10.1145/2600428.2609615

### QMX

Quantities, Multipliers, and eXtractor (QMX) packs as many integers as possible into 128-bit words (Quantities) and stores the selectors (eXtractors) separately in a different stream. The selectors are compressed (Multipliers) with
RLE (Run-Length Encoding).

To compress an index using QMX use the index type `block_qmx`.

> Andrew Trotman. 2014. Compression, SIMD, and Postings Lists. In Proceedings of the 2014 Australasian Document Computing Symposium (ADCS '14), J. Shane Culpepper, Laurence Park, and Guido Zuccon (Eds.). ACM, New York, NY, USA, Pages 50, 8 pages. DOI: https://doi.org/10.1145/2682862.2682870

### SIMD-BP128

> Daniel Lemire, Leonid Boytsov: Decoding billions of integers per second through vectorization. Softw., Pract. Exper. 45(1): 1-29 (2015)

### Simple8b
--------
> 	Vo Ngoc Anh, Alistair Moffat: Index compression using 64-bit words. Softw., Pract. Exper. 40(2): 131-147 (2010)

### Simple16

> Jiangong Zhang, Xiaohui Long, and Torsten Suel. 2008. Performance of compressed inverted list caching in search engines. In Proceedings of the 17th international conference on World Wide Web (WWW '08). ACM, New York, NY, USA, 387-396. DOI: https://doi.org/10.1145/1367497.1367550

### StreamVByte

> Daniel Lemire, Nathan Kurz, Christoph Rupp: Stream VByte: Faster byte-oriented integer compression. Inf. Process. Lett. 130: 1-6 (2018). DOI: https://doi.org/10.1016/j.ipl.2017.09.011

### Varint-G8IU

> Alexander A. Stepanov, Anil R. Gangolli, Daniel E. Rose, Ryan J. Ernst, and Paramjit S. Oberoi. 2011. SIMD-based decoding of posting lists. In Proceedings of the 20th ACM international conference on Information and knowledge management (CIKM '11), Bettina Berendt, Arjen de Vries, Wenfei Fan, Craig Macdonald, Iadh Ounis, and Ian Ruthven (Eds.). ACM, New York, NY, USA, 317-326. DOI: https://doi.org/10.1145/2063576.2063627

### VarintGB

>	Jeffrey Dean. 2009. Challenges in building large-scale information retrieval systems: invited talk. In Proceedings of the Second ACM International Conference on Web Search and Data Mining (WSDM '09), Ricardo Baeza-Yates, Paolo Boldi, Berthier Ribeiro-Neto, and B. Barla Cambazoglu (Eds.). ACM, New York, NY, USA, 1-1. DOI: http://dx.doi.org/10.1145/1498759.1498761
