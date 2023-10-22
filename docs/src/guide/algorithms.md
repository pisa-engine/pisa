# Retrieval Algorithms

This is the list of the supported query processing algorithms.

## Unranked

PISA implements two _unranked_ algorithms, meaning they return a full
list of documents matching the query in the order of their appearance in
the posting lists.

### Intersection

The intersection algorithm (`and`) returns only the documents that match
all query terms.

### Union

The union algorithm (`or`) returns all the documents that match any
query term.

## Top-k Retrieval

Top-k retrieval returns the top-k highest scored documents with respect
To the given query.

### Document-at-a-time (DaaT)

Document-at-a-time algorithms traverse one document at a time. They rely
on posting lists being sorted by document IDs, and scan them in step to
retrieve all frequencies for a document right away.

#### Conjunctive processing

Conjunctive processing (`ranked_and`) returns the top _k_ documents that
contain _all_ of the query terms. This is an exhaustive algorithm,
meaning all documents must be scored.

#### Disjunctive processing

Conjunctive processing (`ranked_or`) returns the top _k_ documents that
contain _any_ of the query terms. This is an exhaustive algorithm,
meaning all documents must be scored.

#### MaxScore

MaxScore (`maxscore`) uses precomputed maximum partial scores for each
term to avoid calculating all scores. It is especially suitable for
longer queries (high term count), short posting lists, or high values of
_k_ (number of returned top documents).

> Howard Turtle and James Flood. 1995. Query evaluation: strategies and
> optimizations. Inf. Process. Manage. 31, 6 (November 1995), 831-850.
> DOI=http://dx.doi.org/10.1016/0306-4573(95)00020-H

#### WAND

Similar to MaxScore, WAND (`wand`) uses precomputed maximum partial
scores for each term to avoid calculating all scores. Its performance is
sensitive to the term count, so it may not be the best choice for long
queries. It may also take a performance hit when _k_ is very high, in
which case MaxScore may prove more efficient.

> Andrei Z. Broder, David Carmel, Michael Herscovici, Aya Soffer, and
> Jason Zien. 2003. Efficient query evaluation using a two-level
> retrieval process. In Proceedings of the twelfth international
> conference on Information and knowledge management (CIKM '03). ACM,
> New York, NY, USA, 426-434. DOI: https://doi.org/10.1145/956863.956944

#### BlockMax WAND

BlockMax WAND (`block_max_wand`) builds on top of WAND. It uses
additional precomputed scores for ranges of documents in posting lists,
which allows for skipping entire blocks of documents if their max score
is low enough.

> Shuai Ding and Torsten Suel. 2011. Faster top-k document retrieval
> using block-max indexes. In Proceedings of the 34th international ACM
> SIGIR conference on Research and development in Information Retrieval
> (SIGIR '11). ACM, New York, NY, USA, 993-1002.
> DOI=http://dx.doi.org/10.1145/2009916.2010048

#### Variable BlockMax WAND

Variable BlockMax WAND is the same algorithm as `block_max_wand` at
query time. The difference is in precomputing the block-max scores.
Instead having even block sizes, each block can have a different size,
to optimize the effectiveness of skips.

> Antonio Mallia, Giuseppe Ottaviano, Elia Porciani, Nicola Tonellotto,
> and Rossano Venturini. 2017. Faster BlockMax WAND with Variable-sized
> Blocks. In Proceedings of the 40th International ACM SIGIR Conference
> on Research and Development in Information Retrieval (SIGIR '17). ACM,
> New York, NY, USA, 625-634. DOI:
> https://doi.org/10.1145/3077136.3080780

#### BlockMax MaxScore

BlockMax MaxScore (`block_max_maxscore`) is a MaxScore implementation
with additional block-max scores, similar to BlockMax WAND.

#### BlockMax AND

BlockMax AND (`block_max_ranked_and`) is a conjunctive algorithm using
block-max scores.

### Term-at-a-time (TaaT)

Term-at-a-time algorithms traverse one posting list at a time. Thus,
they cannot rely on all frequencies for a given document being known at
the time of their processing. This requires an accumulator structure to
keep partial scores.

#### Disjunctive TaaT processing

Disjunctive TaaT (`ranked_or_taat`) is a simple algorithm that
accumulates document scores while traversing postings one list at a
time. `ranked_or_taat_lazy` is a variant that uses an accumulator array
that initializes lazily.
