# Retrieving Query Results

This program is used to retrieve query results.
The output is in TREC format, so it can be directly used with `trec_eval` utility
to evaluate query precision.

## Usage

    Retrieves query results in TREC format.
    Usage: ./bin/evaluate_queries [OPTIONS]

    Options:
      -h,--help                   Print this help message and exit
      --config TEXT               Configuration .ini file
      -t,--type TEXT REQUIRED     Index type
      -i,--index TEXT REQUIRED    Collection basename
      -w,--wand TEXT              Wand data filename
      -q,--query TEXT             Queries filename
      --compressed-wand           Compressed wand input file
      -k UINT                     k value
      --terms TEXT                Term lexicon
      --nostem Needs: --terms     Do not stem terms
      --documents TEXT REQUIRED   Document lexicon
