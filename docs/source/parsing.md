Parsing
=======

## Usage

To parse a collection into a forward index, use `parse_collection` command:

    parse_collection - parse collection and store as forward index.
    Usage: ./bin/parse_collection [OPTIONS]
    
    Options:
      -h,--help                   Print this help message and exit
      -o,--output TEXT REQUIRED   Forward index filename
      -j,--threads UINT           Thread count
      -b,--batch-size INT=100000  Number of documents to process in one thread
      -f,--format TEXT=plaintext  Input format
      --stemmer TEXT              Stemmer type
      --content-parser TEXT       Content parser type

For example:

    $ zcat ClueWeb09B/*/*.warc.gz | \   # pass unzipped stream in WARC format
        parse_collection \
        -j 8 \                          # use up to 8 threads at a time
        -b 10000 \                      # one thread builds up to 10k documents in memory
        -f warc \                       # use WARC
        --stemmer porter2 \             # stem every term using the Porter2 algorithm
        --content-parser html \         # parse HTML content before extracting tokens
        -o cw09b

The above command will write the following files:
- `cw09b`: forward index in binary format,
- `cw09b.terms`: a new-line-delimited list of sorted terms,
  where term having ID N is on line N
- `cw09b.documents`: a new-line-delimited list of document titles (e.g., TREC-IDs),
  where document having ID N is on line N

Right now, only `porter2` stemmer is supported.

Supported formats are:
- `plaintext`: every line contains the document's title first, then any number of
             whitespaces, followed by the content delimited by a new line character,
- `warc`: Web ARChive format as defined in [the format specification](https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.0/)
