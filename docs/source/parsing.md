Parsing
=======

A _forward index_ is a data structure that stores the term identifiers associated to every document. Conversely, an _inverted index_ stores for each unique term the document identifiers where it appears (usually, associated to a numeric value used for ranking purposes such as the raw frequency of the term within the document).

The objective of the parsing process is to represent a given collection as a forward index. To parse a collection, use the `parse_collection` command:

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
      --debug                     Print debug messages

      Subcommands:
        merge                       Merge previously produced batch files.
                                    When parsing process was killed during merging, use this
                                    command to finish merging without having to restart building batches.

For example:

    $ mkdir -p path/to/forward
    $ zcat ClueWeb09B/*/*.warc.gz | \   # pass unzipped stream in WARC format
        parse_collection \
        -j 8 \                          # use up to 8 threads at a time
        -b 10000 \                      # one thread builds up to 10k documents in memory
        -f warc \                       # use WARC
        --stemmer porter2 \             # stem every term using the Porter2 algorithm
        --content-parser html \         # parse HTML content before extracting tokens
        -o path/to/forward/cw09b

In case you get the error `-bash: /bin/zcat: Argument list too long`, you can pass the unzipped stream using:

    $ find ClueWeb09B -name '*.warc.gz' -exec zcat -q {} \;

The parsing process will write the following files:
- `cw09b`: forward index in binary format.
- `cw09b.terms`: a new-line-delimited list of sorted terms,
  where term having ID N is on line N, with N starting from 0.
- `cw09b.documents`: a new-line-delimited list of document titles (e.g., TREC-IDs),
  where document having ID N is on line N, with N starting from 0.
- `cw09b.urls`: a new-line-delimited list of URLs, where URL having ID N is on
  line N, with N starting from 0. Also, keep in mind that each ID corresponds with
  an ID of the `cw09b.documents` file.

### Supported stemmers
- Porter2
- Krovetz

### Supported formats
- `plaintext`: every line contains the document's title first, then any number of
             whitespaces, followed by the content delimited by a new line character.
- `trectext`: TREC newswire collections.
- `trecweb`: TREC web collections.
- `warc`: Web ARChive format as defined in [the format specification](https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.0/).
- `wapo`: TREC Washington Post Corpus.
