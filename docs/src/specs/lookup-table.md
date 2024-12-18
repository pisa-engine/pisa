# Lookup Table Format Specification

A lookup table is a bidirectional mapping from an index, representing an
internal ID, to a binary payload, such as string. E.g., an `N`-element
lookup table maps values `0...N-1` to their payloads. These tables are
used for things like mapping terms to term IDs and document IDs to
titles or URLs.

The format of a lookup table is designed to operate without having to
parse the entire structure. Once the header is parsed, it is possible to
operate directly on the binary format to access the data. In fact, a
lookup table will typically be memory mapped. Therefore, it is possible
to perform a lookup (or reverse lookup) without loading the entire
structure into memory.

The header always begins as follows:

```
+--------+--------+--------   -+
|  0x87  |  Ver.  |        ... |
+--------+--------+--------   -+
```

The first byte is a constant identifier. When reading, we can verify
whether this byte is correct to make sure we are using the correct type
of data structure.

The second byte is equal to the version of the format.

The remaining of the format is defined separately for each version. The
version is introduced in order to be able to update the format in the
future but still be able to read old formats for backwards
compatibility.

## v1

```
+--------+--------+--------+--------+--------+--------+--------+--------+
|  0x87  |  0x01  | Flags  |                    0x00                    |
+--------+--------+--------+--------+--------+--------+--------+--------+
|                                 Length                                |
+--------+--------+--------+--------+--------+--------+--------+--------+
|                                                                       |
|                                Offsets                                |
|                                                                       |
+-----------------------------------------------------------------------+
|                                                                       |
|                                Payloads                               |
|                                                                       |
+-----------------------------------------------------------------------+
```

Immediately after the version bit, we have flags byte.

```
 MSB                         LSB
+---+---+---+---+---+---+---+---+
| 0 | 0 | 0 | 0 | 0 | 0 | W | S |
+---+---+---+---+---+---+---+---+
```

The first bit (`S`) indicates whether the payloads are sorted (1) or not
(0). The second bit (`W`) defines the width of offsets (see below):
32-bit (0) or 64-bit (1). In most use cases, the cumulative size of the
payloads will be small enough to address it by 32-bit offsets. For
example, if we store words that are 16-bytes long on average, we can
address over 200 million of them. For this many elements, reducing the
width of the offsets would save us over 700 MB. Still, we want to
support 64-bit addressing because some payloads may be much longer
(e.g., URLs).

The rest of the bits in the flags byte are currently not used, but
should be set to 0 to make sure that if more flags are introduced, we
know what values to expect in the older iterations, and thus we can make
sure to keep it backwards-compatible.

The following 5 bytes are padding with values of 0. This is to help with
byte alignment. When loaded to memory, it should be loaded with 8-byte
alignment. When memory mapped, it should be already correctly aligned by
the operating system (at least on Linux).

Following the padding, there is a 64-bit unsigned integer encoding the
number of elements in the lexicon (`N`).

Given `N` and `W`, we can now calculate the byte range of all offsets,
and thus the address offset for the start of the payloads. The offsets
are `N+1` little-endian unsigned integers of size determined by `W`
(either 4 or 8 bytes). The offsets are associated with consecutive IDs
from 0 to `N-1`; the last the `N+1` offsets points at the first byte
after the last payload. The offsets are relative to the beginning of the
first payload, therefore the first offset will always be 0.

Payloads are arbitrary bytes, and must be interpreted by the software.
Although the typical use case are strings, this can be any binary
payload. Note that in case of strings, they will not be 0-terminated
unless they were specifically stored as such. Although this should be
clear by the fact a payload is simply a sequence of bytes, it is only
prudent to point it out. Thus, one must be extremely careful when using
C-style strings, as their use is contingent on a correct values inserted
and encoded in the first place, and assuming 0-terminated strings may
easily lead to undefined behavior. Thus, it is recommended to store
strings without terminating them, and then interpret them as string
views (such as `std::string_view`) instead of a C-style string.

The boundaries of the k-th payload are defined by the values of k-th and
(k+1)-th offsets. Note that because of the additional offset that points
to immediately after the last payload, we can read offsets `k` and `k+1`
for any index `k < N` (recall that `N` is the number of elements).

If the payloads are sorted (S), we can find an ID of a certain payload
with a binary search. This is crucial for any application that requires
mapping from payloads to their position in the table.
