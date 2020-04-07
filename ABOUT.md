<p align="center"><img src="https://pisa-engine.github.io/images/logo250.png" width="250px"></p>

# PISA: Performant Indexes and Search for Academia

## What is PISA?
PISA is a text search engine, though the "PISA Project" is a
set of tools that help experiment with indexing and query processing.
Given a text collection, PISA can build an inverted index over this corpus,
allowing the corpus to be searched. At query time, PISA stores its index in
main memory for rapid retrieval.

### What does that all mean?
In very simple terms, PISA is a text search engine. Starting with a corpus of
documents, for example, Wikipedia, PISA can build an *inverted index* which allows us
to rapidly search the Wikipedia collection. At the most basic level, Boolean
`AND` and `OR` queries are supported. Imagine we wanted to find all of the
Wikipedia documents matching the query *"oolong tea"* - we could run a
Boolean conjunction (*oolong* `AND` *tea*). We might instead be interested in
finding documents containing either *oolong* or *tea* (or both), in which case
we can run a Boolean disjunction (*oolong* `OR` *tea*). 

Beyond simple Boolean matching, as discussed above, we can actually *rank*
documents. Without going into details, documents are ranked by functions that
assume the more *rare* a term is, the more *important* the word is. These
rankers also assume that the more often a word appears in a document, the
more likely the document is to be about that word. Finally, longer documents
contain more words, and are therefore more likely to get higher scores than
shorter documents, so normalization is conducted to ensure all documents are
treated equally. The interested reader may wish to examine the [TF/IDF](https://en.wikipedia.org/wiki/Tf%E2%80%93idf)
Wikipedia article to learn more about this method of ranking.

### Search vs pattern matching?
This is not the same type of search `grep` for example. This is more closely
related to the popular Lucene search engine, though we don't currently support 
as many query types as Lucene. As discussed previously, the main underlying data
structure in PISA is the *inverted index*. The inverted index stores, for each
term, a list of documents that contain that term. These lists can be rapidly
traversed to find documents that match the query terms, and these documents can
then be scored, ranked, and returned to the user.

### Who is PISA aimed at?
The primary use-case for PISA is to conduct experiments to further the understanding
of the field of Information Retrieval (IR). 
Within the field of IR, there are various
important research directions that are focused on, from improving results
quality (effectiveness), to improving the scalability and efficiency of search
systems. PISA is focused mostly on the scalability and efficiency side of
IR research, and is why PISA stands for "Performant Indexes and Search for Academia". 
In short, PISA is a platform for developing new innovations in efficient search.

### What if I just want to play with a search engine?
While PISA is focused on being a base for experimentation, it is also perfectly
suitable for use as a simple general purpose indexing and search system.

### What sort of scale can PISA handle?
PISA can handle large text collections. For example, PISA can easily index the
ClueWeb09B or ClueWeb12B corpora, which each contain over $50$ million web
documents and close to 500 GiB of *compressed* textual data, resulting in 
indexes that are in the range of 10-40 GiB, depending on the compression
codec used. These indexes, depending on some details, can be built from scratch
in around 10-20 hours. 
In addition, larger collections can be handled via *index sharding*
which breaks large corpora into smaller subsets (shards).
We do note, however, that PISA is an *in-memory* system, which aims to serve
queries based on an index residing entirely in main memory. As such, the possible
scale will be limited by main memory. 

### How fast is PISA, really?
A few recent works have benchmarked PISA. For example, 
*An Experimental Study of Index Compression and DAAT Query Processing Methods*
by Antonio Mallia, Michal Siedlaczek, and Torsten Suel, which appeared in
ECIR 2019, showed PISA to be capable of returning the top 10 and top 1000
documents with an average latency in the range of 10-40 and 20-50 *milliseconds*
respectively, on a collection containing 50 million web documents. 

PISA is also included in Tantivy's [search engine benchmark game](https://tantivy-search.github.io/bench/), which also
has [Tantivy](https://github.com/tantivy-search), [Lucene](https://lucene.apache.org/), and [Rucene](https://github.com/zhihu/rucene) as competitors.

## How did PISA begin?
PISA is a fork of the [ds2i](https://github.com/ot/ds2i/) project started by [Giuseppe Ottaviano](https://github.com/ot).
The ds2i project contained the source code for a number of important efficiency
innovations in IR, including the "Partitioned Elias-Fano" compression method.


## Getting started
For those interested in working with PISA, we suggest examining the following
resources:

1. The [Open Source Information Retrieval Replicability Challenge (OSIRRC) paper describing PISA](http://ceur-ws.org/Vol-2409/docker08.pdf), including end-to-end experimentation.
2. The accompanying [Docker image](https://github.com/osirrc/pisa-docker) which allows the experiments from above to be replicated.
3. The [documentation](https://pisa.readthedocs.io/en/latest/).
4. Drop in to our [Slack channel](https://join.slack.com/t/pisa-engine/shared_invite/zt-dbxrm1mf-RtQMZTqxxlhOJsv3GHUErw) and say hi!
