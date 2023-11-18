# "WAND" Data

> **WARNING** \
> "WAND data" is a legacy term that may be somewhat misleading. The name
> originates from the WAND algorithm, but in fact all _scored_ queries
> must use WAND data. This name will likely change in the future to more
> accurately reflect its nature, but we still use it now because it is
> prevalent throughout the code base.

This is a file that contains data necessary for scoring documents, such
as:
* document lengths,
* term occurrence counts,
* term posting counts,
* number of documents,
* term max scores,
* (optional) term block-max scores.

Use [`create_wand_data`](../cli/create_wand_data.html) command to build.

## Quantization

If you quantize your inverted index, then you have to quantize WAND data
as well, using the same number of bits (otherwise, any algorithms that
depend on max-scores will not function correctly).

## Compression

You can build your WAND data with score compression. This will store
scores quantized, thus you need to provide the number of quantization
bits. Note that even if you compress your WAND data, you can still use
it with a non-quantized index: the floating point scores will be
calculated (though they will not be identical to the original scores,
as this compression is _lossy_). If you do use a quantized index, it
must use the same number of bits as WAND data.
