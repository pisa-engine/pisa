> This document is a **work in progress**.

# Introduction

In our efforts to come up with the v1.0 of both PISA and our index format,
we should start a discussion about the shape of things from the point of view
of both the binary format and how we can use it in our library.

## Index Format specification

This document mainly discusses the binary file format of each index component,
as well as how these components come together to form a cohesive structure.

## Reference Implementation

Along with format description and discussion, this directory includes some
reference implementation of the discussed structures and some algorithms working on them.

The goal of this is to show how things work on certain examples,
and find out what works and what doesn't and still needs to be thought through.

> Look in `test/test_v1.cpp` for code examples.

# Posting Files

> Example: `v1/raw_cursor.hpp`.

Each _posting file_ contains a list of blocks of data, each related to a single term,
preceded by a header encoding information about the type of payload.

> Do we need the header? I would say "yes" because even if we store the information
> somewhere else, then we might want to (1) verify that we are reading what we think
> we are reading, and (2) verify format version compatibility.
> The latter should be further discussed.

```
Posting File := Header, [Posting Block]
```

Each posting block encodes a list of homogeneous values, called _postings_.
Encoding is not fixed.

> Note that _block_ here means the entire posting list area.
> We can work on the terminology.

## Header

> Example: `v1/posting_format_header.hpp`.

We should store the type of the postings in the file, as well as encoding used.
**This might be tricky because we'd like it to be an open set of values/encodings.**

```
Header := Version, Type, Encoding
Version := Major, Minor, Path
Type := ValueId, Count
```

## Posting Types

I think supporting these types will be sufficient to express about anything we
would want to, including single-value lists, document-frequency (or score) lists,
positional indexes, etc.

```
Type := Primitive | List[Type] | Tuple[Type]
Primitive := int32 | float32
```

## Encodings

We can identify encodings by either a name or ID/hash, or both.
I can imagine that an index reader could **register** new encodings,
and default to whatever we define in PISA.
We should then also verify that this encoding implement a `Encoding<Type>` "concept".
This is not the same as our "codecs".
This would be more like posting list reader.

> Example: `IndexRunner` in `v1/index.hpp`.
