<p align="center"><img src="https://pisa-engine.github.io/images/logo250.png" width="250px"></p>

# PISA: Performant Indexes and Search for Academia v0.8.2

![Build and test](https://github.com/pisa-engine/pisa/workflows/Build%20and%20test/badge.svg)
![clang-tidy](https://github.com/pisa-engine/pisa/workflows/clang-tidy/badge.svg)
[![Documentation Status](https://readthedocs.org/projects/pisa/badge/?version=latest)](https://pisa.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/pisa-engine/pisa/branch/master/graph/badge.svg)](https://codecov.io/gh/pisa-engine/pisa)


[![GitHub issues](https://img.shields.io/github/issues/pisa-engine/pisa.svg)](https://github.com/pisa-engine/pisa/issues)
[![GitHub forks](https://img.shields.io/github/forks/pisa-engine/pisa.svg)](https://github.com/pisa-engine/pisa/network)
[![GitHub stars](https://img.shields.io/github/stars/pisa-engine/pisa.svg)](https://github.com/pisa-engine/pisa/stargazers)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/pisa-engine/pisa/pulls)
[![DOI](https://zenodo.org/badge/150449350.svg)](https://zenodo.org/badge/latestdoi/150449350)

## Join us on Slack
Get in touch via Slack: [![Slack](https://img.shields.io/badge/slack-join-blue.svg)](https://join.slack.com/t/pisa-engine/shared_invite/zt-dbxrm1mf-RtQMZTqxxlhOJsv3GHUErw)

## Overview

PISA is a text search engine able to run on large-scale collections of documents. It allows researchers to experiment with state-of-the-art techniques, allowing an ideal environment for rapid development.

Some features of PISA are listed below:

* Written in C++ for performance;
* Parsing, Indexing, and Sharding capabilities;
* Many index compression methods implemented;
* Many query processing algorithms implemented;
* Implementation of document reordering;
* Free and open-source with permissive license;

## About PISA

### What is PISA?
PISA is a text search engine, though the "PISA Project" is a
set of tools that help experiment with indexing and query processing.
Given a text collection, PISA can build an *inverted index* over this corpus,
allowing the corpus to be searched. The inverted index, put simply, is an efficient
data structure that represents the document corpus by storing a list of documents
for each unique term (see [here](https://en.wikipedia.org/wiki/Search_engine_indexing#Inverted_indices)).
At query time, PISA stores its index in main memory for rapid retrieval.

#### What does that all mean?
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

#### Search vs pattern matching
This is not the same type of search `grep` for example. This is more closely
related to the popular Lucene search engine, though we don't currently support 
as many query types as Lucene. As discussed previously, the main underlying data
structure in PISA is the *inverted index*. The inverted index stores, for each
term, a list of documents that contain that term. These lists can be rapidly
traversed to find documents that match the query terms, and these documents can
then be scored, ranked, and returned to the user.

#### Who should use PISA?
The primary use-case for PISA is to conduct experiments to further the understanding
of the field of Information Retrieval (IR). 
Within the field of IR, there are various
important research directions that are focused on, from improving results
quality (effectiveness), to improving the scalability and efficiency of search
systems. PISA is focused mostly on the scalability and efficiency side of
IR research, and is why PISA stands for "Performant Indexes and Search for Academia". 
In short, PISA is a platform for developing new innovations in efficient search.

#### What if I just want to play with a search engine?
While PISA is focused on being a base for experimentation, it is also perfectly
suitable for use as a simple general purpose indexing and search system.

#### What sort of scale can PISA handle?
PISA can handle large text collections. For example, PISA can easily index the
ClueWeb09B or ClueWeb12B corpora, which each contain over 50 million web
documents and close to 500 GiB of *compressed* textual data, resulting in 
indexes that are in the range of 10-40 GiB, depending on the compression
codec used. These indexes, depending on some details, can be built from scratch
in around 10-20 hours. 
In addition, larger collections can be handled via *index sharding*
which breaks large corpora into smaller subsets (shards).
We do note, however, that PISA is an *in-memory* system, which aims to serve
queries based on an index residing entirely in main memory. As such, the possible
scale will be limited by main memory. 

#### How fast is PISA, really?
A few recent works have benchmarked PISA. For example, 
*An Experimental Study of Index Compression and DAAT Query Processing Methods*
by Antonio Mallia, Michal Siedlaczek, and Torsten Suel, which appeared in
ECIR 2019, showed PISA to be capable of returning the top 10 and top 1000
documents with an average latency in the range of 10-40 and 20-50 *milliseconds*
respectively, on a collection containing 50 million web documents. 

PISA is also included in Tantivy's [search engine benchmark game](https://tantivy-search.github.io/bench/), which also
has [Tantivy](https://github.com/tantivy-search), [Lucene](https://lucene.apache.org/), and [Rucene](https://github.com/zhihu/rucene) as competitors.

### How did PISA begin?
PISA is a fork of the [ds2i](https://github.com/ot/ds2i/) project started by [Giuseppe Ottaviano](https://github.com/ot).
The ds2i project contained the source code for a number of important efficiency
innovations in IR, including the "Partitioned Elias-Fano" compression method.


### Getting Started
For those interested in working with PISA, we suggest examining the following
resources:

1. The [Open Source Information Retrieval Replicability Challenge (OSIRRC) paper describing PISA](http://ceur-ws.org/Vol-2409/docker08.pdf), including end-to-end experimentation.
2. The accompanying [Docker image](https://github.com/osirrc/pisa-docker) which allows the experiments from above to be replicated.
3. The [documentation](https://pisa.readthedocs.io/en/latest/).
4. Drop in to our [Slack channel](https://join.slack.com/t/pisa-engine/shared_invite/zt-dbxrm1mf-RtQMZTqxxlhOJsv3GHUErw) and say hi!

If you want to get involved with PISA, please check out our [Contributing](https://github.com/pisa-engine/pisa/blob/master/.github/CONTRIBUTING.md) page.


## Reference

If you use PISA in a research paper, please cite the following reference:
```
@inproceedings{MSMS2019,
  author    = {Antonio Mallia and Michal Siedlaczek and Joel Mackenzie and Torsten Suel},
  title     = {{PISA:} Performant Indexes and Search for Academia},
  booktitle = {Proceedings of the Open-Source {IR} Replicability Challenge co-located
               with 42nd International {ACM} {SIGIR} Conference on Research and Development
               in Information Retrieval, OSIRRC@SIGIR 2019, Paris, France, July 25,
               2019.},
  pages     = {50--56},
  year      = {2019},
  url       = {http://ceur-ws.org/Vol-2409/docker08.pdf}
}
```
