# `compress_inverted_index`

## Usage

```
<!-- cmdrun ../../../build/bin/compress_inverted_index --help -->
```

## Description

Compresses an inverted index from the uncompressed format using one of
the integer encodings.

### Input

The input to this command is an uncompressed version of the inverted
index [described here](../guide/inverting.html#inverted-index-format).
The `--collection` option takes the _basename_ of the uncompressed
index.

### Encoding

The postings are compressed using one of the available integer
encodings, defined by `--encoding`. The available encoding values are:
* `block_interpolative`: [Binary Interpolative
  Coding](../guide/compressing.html#binary-interpolative-coding)
* `ef`: [Elias-Fano](../guide/compressing.html#elias-fano)
* `block_maskedvbyte`: [MaskedVByte](../guide/compressing.html#maskedvbyte)
* `block_optpfor`: [OptPForDelta](../guide/compressing.html#optpfd)
* `pef`: [Partitioned
  Elias-Fano](../guide/compressing.html#partitioned-elias-fano)
* `block_qmx`: [QMX](../guide/compressing.html#qmx)
* `block_simdbp`: [SIMD-BP128](../guide/compressing.html#simd-bp128)
* `block_simple8b`: [Simple8b](../guide/compressing.html#simple8b)
* `block_simple16`: [Simple16](../guide/compressing.html#simple16)
* `block_streamvbyte`: [StreamVByte](../guide/compressing.html#streamvbyte)
* `block_varintg8iu`: [Varint-G8IU](../guide/compressing.html#varint-g8iu)
* `block_varintgb`: [Varint-GB](../guide/compressing.html#varintgb)

### Precomputed Quantized Scores

At the time of compressing the index, you can replace frequencies with
quantized precomputed scores. To do so, you must define `--quantize`
flag, plus some additional options:
* `--scorer`: scoring function that should be used in to calculate the
  scores (`bm25`, `dph`, `pl2`, `qld`)
* `--wand`: metadata filename path
