# Efficient In-Memory Inverted Indexes

## Prerequisites

### Download input data

First, [download the data package](https://www.dropbox.com/scl/fi/kscnjyuh3dsmze0t2lf04/sigir25.zip?rlkey=lkblugqtb7myvsk5h6a54g06n&st=r133t9s0&dl=0). It is around 1.5GB, and about 5GB once decompressed.

Move it somewhere on your machine, and unpack it. Make a note of the full path as we will need it shortly.

    mkdir -p "$HOME/sigir25-input-data"
    mv sigir25.zip $HOME/sigir25-input-data
    cd $HOME/sigir25-input-data
    unzip sigir25.zip
        
### Download container image

Next,
[download the image](https://sigir2025.it-mil-1.linodeobjects.com/pisa-tutorial.tar.gz).
Then, load it locally:

```shell
docker load < pisa-tutorial.tar.gz
```

You can also use any compatible container management tool, such as `podman`.

### Build container image

**Alternatively**, the container image can be built, as shown below.
However, it may take a while to compile everything, so we recommend
downloading the image as shown above.

```shell
git clone https://github.com/pisa-engine/pisa.git
cd pisa
docker build -t pisa-tutorial -f- . < test/docker/tutorial/Dockerfile
```

## Running PISA in container

You can use Docker or Podman to start the container. In the example
below, we mount a volume so that we can persist the files we create
after the container has terminated. We use a mounted volume to persist
the data on the local drive in an arbitrary directory
`$HOME/pisa-workdir` (can be any other directory on your local drive).
We also mount the input volume that points to the input data directory
that should have been downloaded as one of the prerequisites (in this
example, we use `$HOME/sigir25-input-data`).

    mkdir -p "$HOME/pisa-workdir"
    docker image list # Get the specific name of the docker image
    docker run \
        --detach -it                            # run in background
        --network host \                        # to avoid connection issues
        --name=pisa \                           # name container
        -v "$HOME/pisa-workdir:/workdir" \      # mount workdir volume
        -v "$HOME/sigir25-input-data:/input" \  # mount input data volume
        pisa-tutorial                           # image name -- ensure it matches the output of the previous command

    
We can now execute the container in an interactive terminal:

    docker exec -it pisa /bin/bash

You should be able to sanity check the input directory is mounted as expected:
    
    find input/

It should return the contents of the uncompressed data zip from the prerequisites step.
For example:

    root@temple:/# find input/
    input/
    input/lsr-small
    input/lsr-small/spladev3.queries
    input/lsr-small/spladev3-marco-v1.subset.bp.ciff
    input/lsr-small/di-marco-v1.subset.bp.ciff
    input/lsr-small/di.queries
    input/lsr-small/qrels.msmarco-passage.dev-subset.txt
    input/lsr-small/README.md
    input/tf-index
    input/tf-index/marco-v1.bp.ciff
    input/tf-index/dev.queries
    input/tf-index/qrels.msmarco-passage.dev-subset.txt
    input/tf-index/README.md
    root@temple:/# 


### Note about RedHat based systems

If you are running a RedHat based system such as RHEL or Fedora, you may
need to run `sudo setenforce 0` for Podman.

## Section 1: Basic Usage

This section showcases the basic usage of PISA, including indexing a
collection, inspecting metadata, and querying the index. We will start
by indexing a toy collection using default parameters. We will then
explain the constructed artifacts and metadata. Once the index is built,
we will query the index.

Once we familiarize ourselves with the basic commands and concepts, we
will index a larger collection. We will take advantage of the
[`ir-datasets`](https://ir-datasets.com/) integration to obtain the
data. We will then explore querying the collection with multiple
retrieval algorithms and show the difference in performance. Finally, we
will build another index based on the same collection and compare size
and benchmarking results.

### Note on `pisa` command

Throughout this tutorial, we use `pisa` command for easy access to
indexing an querying capabilities of PISA. Note, however, this is an
experimental tool, written in Python, that calls various PISA commands.
It is subject to changes in the future. We will mention some of the
lower-level commands later. For those interested, you can find more
details about all PISA commands in
[our documentation](https://pisa-engine.github.io/pisa/book)'s CLI
Reference section.


### Toy Example

First, let's have a peek at our toy collection. Our file is in JSONL
format -- each line contains a JSON representing a document. We can use
the `jq` tool to pretty-print it:

    cat /input/tiny.jsonl | jq

Each document has a title and content, which is all we will need to
build an index. We can now pipe it to the `pisa` tool with the
appropriate parameters.

    pisa index stdin --format jsonl -o /workdir/toy < /input/tiny.jsonl

Let's break it down. We execute the `index` command, with `stdin`
subcommand, indicating that the collection will be read from the
standard input. We also specify `--format jsonl` in order to employ the
correct parser, as well as the output directory. Finally, we redirect
the content of our file to the program.

The collection is small, so it should finish almost immediately. We can
verify by listing the contents of the directory:

    ls /workdir/toy

We will get back to those later.

### WikIR collection

We can now proceed to indexing a real dataset. For convenience, we will
use the integration with [`ir-datasets`](https://ir-datasets.com/). All
we have to do is to provide the name of the dataset and which field(s) we
want to use as the content. We will use the English subset of the
[WikIR collection](https://ir-datasets.com/wikir.html).

    pisa index ir-datasets wikir/en1k \
        --content-fields text \
        -o /workdir/wikir

This time, it will take a while to index, as this collection contains
roughly 370,000 documents. The collection will also have to be
downloaded (~165 MiB) before the indexing starts. That said, the entire
command shouldn't run much longer than a couple of minutes.

Once it finishes, we can query the index. To avoid passing the index
location as a command argument, we can change directory to where the
index was built:

    cd /workdir/wikir
    pisa query <<< 'hello world'

We should get the results in the TREC format.

It is more useful to use the set of queries that come with the
collection. For convenience, a file in a format understood by PISA is
available at `/input/wikir.queries`. We can run these queries against
the index:

    pisa query < /input/wikir.queries > wikir.results

Notice that we redirect the results to `wikir.results` file. We can now
use the `trec_eval` tool to calculate the relevance metrics. We have to
pass the known query relevance file that is located in the input
directory.

    trec_eval /input/wikir.qrels wikir.results

We can also run a benchmark, instead of returning results, by simply
passing `--benchmark` flag. It also may be more convenient to read the
queries from a file:

    pisa query --benchmark < /input/wikir.queries

This will print out some logs and a JSON result with some statistics.
Note that by default the algorithm used is `block_max_wand`, which is a
rather efficient disjunctive top-k retrieval algorithm. For now, let's
see how the results will compare when we use `ranked_or`, which is
exhaustive retrieval:

    pisa query --benchmark --algorithm ranked_or < /input/wikir.queries

This should be significantly slower. We can also try `ranked_and` for
_conjunctive_ retrieval, which requires _all_ query terms to be present
for a document to be returned:

    pisa query --benchmark --algorithm ranked_and < /input/wikir.queries

By default we will retrieve top 10 results, but we can choose a
different value:

    pisa query --benchmark -k 1000 < /input/wikir.queries

#### Multiple indexes

When building the index, we used default values for the encoding, skip
list block size, and the scorer. We can inspect those by running the
following command:

    pisa meta alias default

We can choose different parameters at indexing time, however, we can also
create an additional index using an alias.

    pisa add-index --alias interpolative --encoding block_interpolative

Notice that this time around it is much faster than before. This is
because much of work can be reused. The collection is already parsed and
an inverted index is built; we only need to compress its postings with a
different encoding.

We can now list aliases:

    pisa meta aliases

And inspect parameters of the new index:

    pisa meta alias interpolative

The `block_interpolative` encoding is known to be much more
space-efficient but slower. We can verify the size by listing the files:

    du -ah /workdir/wikir | grep 'inv:'

Finally, we can query the new index by passing the alias:

    pisa query --benchmark --alias interpolative < /data/queries.txt

Querying the new index should be slower than the initial one.

## Section 2: MS MARCO Experiments

In this section, we will run experiments on the MS MARCO dataset. The
collections are provided in the input directory in the CIFF format.

First, let's build a traditional index with frequencies.

    pisa index ciff \
        --input /input/tf-index/marco-v1.bp.ciff \
        --output /workdir/msmarco

Once done, we can query the index to obtain the results, and calculate
the evaluation metrics:

    cd /workdir/msmarco
    pisa query < /input/tf-index/dev.queries > dev.results
    trec_eval /input/tf-index/qrels.msmarco-passage.dev-subset.txt dev.results

Finally, we can run some benchmarks.

    pisa query --benchmark < /input/tf-index/dev.queries
    
### LSR: DeeperImpact

Similar to the earlier experiments, we can also experiment with learned sparse indexes.
Firstly, we will index [DeeperImpact](https://arxiv.org/html/2405.17093v2). Note that to ensure the tutorial data is not too large, we have only exported the subset of the postings lists required for the following query file.

Since learned sparse retrieval models typically provide the per-term impact for each document, we do not need to specify a ranker. As an alternative, we must instruct PISA to just use the weights provided, and we can do that here with the `--scorer passthrough` flag.

    pisa index ciff \
        --input /input/lsr-small/di-marco-v1.subset.bp.ciff \
        --output /workdir/di \
        --scorer passthrough

As before, we can now query our index:

    cd /workdir/di
    pisa query < /input/lsr-small/di.queries > di.results
    trec_eval /input/lsr-small/qrels.msmarco-passage.dev-subset.txt di.results
    
Similarly, we can benchmark our queries. Note we are just using a subset of 100 queries here:

    pisa query --benchmark < /input/lsr-small/di.queries

If you wish, you can re-run the commands above specifying different compression methods, querying algorithms, and so on.

### LSR: Splade

Next, we will also index the [SPLADEv3](https://arxiv.org/abs/2403.06789) model. Once again, we've only provided the necessary postings to keep the ciff file small, and we must use the `passthrough` option.

    pisa index ciff \
        --input /input/lsr-small/spladev3-marco-v1.subset.bp.ciff \
        --output /workdir/splade \
        --scorer passthrough

Query: Unlike DeeperImpact, we also need to specify weighted querying for SPLADE, as the *query* term weights must be obeyed for effective retrieval.
Examining the queries file will make this evident:
    
    head -n 1 /input/lsr-small/spladev3.queries 
    2:and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and and is is is is is is is is is is is is is is is is is is is is is is is is is ##r ##r ##r ##r ##r ##r ##r ##r ##r ##r ##r ##r ##r ##r ##r ##r ##r ##r ##r ##r ##r ##r alex alex alex alex alex alex alex alex alex alex alex alex alex alex alex alex plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus plus gene gene gene alan alan alan alan alan alan alan alan alan alan alan alan alan alan alan alan alan alan alan alan alan alan alan alan alan alan alan alan emma emma emma emma definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition definition lily lily lily lily lily lily lily lily lily lily lily lily lily lily lily lily lily lily lily lily lily lily lily lily lily lily lily lily lily lily biology biology biology biology biology biology biology biology biology biology biology biology biology biology biology biology biology biology biology biology biology biology biology biology biology biology biology biology biology biology biology biology biology biology biology biology biology biology biology biology biology biology pedro pedro pedro pedro pedro pedro pedro pedro enzyme enzyme enzyme enzyme enzyme enzyme enzyme substance substance substance substance substance substance substance substance substance substance substance substance substance substance substance substance substance substance substance substance substance gage gage gage gage gage gage gage receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptor receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors receptors ser ser ser ser ser ser ser ser ser ser ser ser ser ser ser ser ser ser ser ser ser ser ser ser ser ser ser ant pill pill pill pill pill pill pill pill pill pill pill pill pill pill pill pill pill pill pill pill pill pill pill pill pill pill pill hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone hormone ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen ##rogen gland gland gland gland gland gland gland gland

Let us now try query processing:

    cd /workdir/splade
    pisa query \
        --weighted \ # must use weighted queries for splade
        < /input/lsr-small/spladev3.queries > spladev3.results
    trec_eval /input/lsr-small/qrels.msmarco-passage.dev-subset.txt spladev3.results

## Try it Yourself
Both DeeperImpact and SPLADEv3 have been exported with the same subset of queries.
Try to benchmark the two indexes - which is faster? Which is more effective?
Experiment with different values of k, different retrieval algorithms, and try turning term weighting on and off!
 
## Appendix: Finer Details

TODO(<https://github.com/pisa-engine/pisa/issues/611>)

Below are brief descriptions of some of the files you will see, and what their role is:

* `metadata.yaml`: collection metadata
* `documents`: new-line separated document titles
* `terms`: new-line separated terms occurring in the collection
* `urls`: new-line separated URLs (empty lines for this example)
* `doclex`: document lexicon -- a mapping between document titles and
  numeric document IDs in a binary format
* `termlex`: term lexicon -- a mapping between terms and numeric term
  IDs in a binary format
* `fwd`: forward index
* `inv.docs`: inverted index (document IDs)
* `inv.freqs`: inverted index (frequencies)
* `inv.sizes`: document sizes (binary format)
* `inv:block_simdbp`: inverted index in a binary format
* `wdata:size=64:bm25:b=0.4:k1=0.9`: additional data, including max
  scores and skip lists
