# Threshold Estimation

Currently it is possible to perform threshold estimation tasks using the
`kth_threshold` tool. The tool computes the k-highest impact score for
each term of a query. Clearly, the top-k threshold of a query can be
lower-bounded by the maximum of the k-th highest impact scores of the
query terms.

In addition to the k-th highest score for each individual term, it is
possible to use the k-th highest score for certain pairs and triples of
terms.

To perform threshold estimation use the `kth_threshold` command.
