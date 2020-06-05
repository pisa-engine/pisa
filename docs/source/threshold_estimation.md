Threshold estimation
=======

Currently it is possible to perform threshold estimation tasks using the `kth_threshold` tool.
The tool computes the k-highest impact score for each term of a query. Clearly, the top-k threshold of a query can be lower-bounded by the maximum of the k-th highest impact scores of the query terms.

In addition to the k-th highest score for each individual term, it is possible to use the k-th highest score for certain pairs and triples of terms.

To perform threshold estimation use the `kth_threshold` command:
```bash
A tool for performing threshold estimation using the k-highest impact score for each term, pair or triple of a query. Pairs and triples are only used if provided with --pairs and --triples respectively.
Usage: ./bin/kth_threshold [OPTIONS]

Options:
  -h,--help                   Print this help message and exit
  -e,--encoding TEXT REQUIRED Index encoding
  -i,--index TEXT REQUIRED    Inverted index filename
  -w,--wand TEXT REQUIRED     WAND data filename
  --compressed-wand Needs: --wand
                              Compressed WAND data file
  -q,--queries TEXT           Path to file with queries
  --terms TEXT                Term lexicon
  --stopwords TEXT Needs: --terms
                              List of blacklisted stop words to filter out
  --stemmer TEXT Needs: --terms
                              Stemmer type
  -k INT REQUIRED             The number of top results to return
  -s,--scorer TEXT REQUIRED   Scorer function
  --bm25-k1 FLOAT Needs: --scorer
                              BM25 k1 parameter.
  --bm25-b FLOAT Needs: --scorer
                              BM25 b parameter.
  --pl2-c FLOAT Needs: --scorer
                              PL2 c parameter.
  --qld-mu FLOAT Needs: --scorer
                              QLD mu parameter.
  --config TEXT               Configuration .ini file
  -p,--pairs TEXT Excludes: --all-pairs
                              A tab separated file containing all the cached term pairs
  -t,--triples TEXT Excludes: --all-triples
                              A tab separated file containing all the cached term triples
  --all-pairs Excludes: --pairs
                              Consider all term pairs of a query
  --all-triples Excludes: --triples
                              Consider all term triples of a query
  --quantized                 Quantizes the scores
```

`--all-pairs` and `--all-triples` can be used if you want to consider all the pairs and triples terms of a query as being previously cached.