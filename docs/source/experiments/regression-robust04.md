# PISA: Regressions for [Disks 4 & 5](https://trec.nist.gov/data_disks.html) (Robust04)

## Indexing

First, we will create a directory where all the indexes are going to be stored:

```
mkdir robust04
```

### Parsing

```
gzip -dc $(find /path/to/disk45/ -type f -name '*.*z' \( -path '*/disk4/fr94/[0-9]*/*' -o -path '*/disk4/ft/ft*' -o -path '*/disk5/fbis/fb*' -o -path '*/disk5/latimes/la*' \)) | bin/parse_collection -f trectext -b 10000 --stemmer porter2 --content-parser html -o robust04/fwd
```

You can replace `gzip -dc` with `zcat` on Linux or `gzcat` on MacOS.
The directory `/path/to/disk45/` should be the root directory of [TREC Disks 4 & 5](https://trec.nist.gov/data_disks.html).

### Inverting

```
bin/invert -i robust04/fwd -o robust04/inv -b 400000 --term-count $(cat robust04/fwd.terms | wc -l)
```

### Reordering

```
bin/recursive_graph_bisection -c robust04/inv -o robust04/inv.bp --documents robust04/fwd.doclex --reordered-documents robust04/fwd.bp.doclex
```

### Meta data

```
bin/create_wand_data -c robust04/inv.bp -b 64 -o robust04/inv.bm25.bmw -s bm25
```

### Index Compression

```
bin/create_freq_index -e block_simdbp -c robust04/inv.bp -o robust04/inv.block_simdbp --check
```
## Retrieval

Queries can be downloaded from NIST:
[TREC 2004 Robust Track (Topics 301-450 & 601-700)](http://trec.nist.gov/data/robust/04.testset.gz)

```
wget http://trec.nist.gov/data/robust/04.testset.gz
gunzip 04.testset.gz
bin/extract_topics -i 04.testset -o topics.robust2004
```
The above command will download the topics from the NIST website, extract the archive and parse topics in order to get `title`, `desc` and `narr` fields in separate files.

```
bin/evaluate_queries -e block_simdbp -a block_max_wand -i robust04/inv.block_simdbp -w robust04/inv.bm25.bmw --stemmer porter2 --documents robust04/fwd.bp.doclex --terms robust04/fwd.termlex -k 1000 --scorer bm25 -q topics.robust2004.title > run.robust2004.bm25.title.robust2004.txt
```

## Evaluation

Qrels can be downloaded from NIST:
[TREC 2004 Robust Track (Topics 301-450 & 601-700)](http://trec.nist.gov/data/robust/qrels.robust2004.txt)


```
wget http://trec.nist.gov/data/robust/qrels.robust2004.txt
trec_eval -m map -m P.30 -m ndcg_cut.20 qrels.robust2004.txt run.robust2004.bm25.title.robust2004.txt
```

With the above commands, you should be able to replicate the following results:

```
map                     all 0.2543
P_30                    all 0.3139
ndcg_cut_20             all 0.4250
```

## Replication Log

+ Results replicated by [@amallia](https://github.com/amallia) on 2020-04-03 (commit [b01073](https://github.com/pisa-engine/pisa/commit/2b010731e6ea1b45a5f4a7caa9135a76219ed487))
