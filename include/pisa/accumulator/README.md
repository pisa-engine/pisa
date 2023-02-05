# Score Accumulators

Score accumulators are used to accumulate (and later aggregate) document
scores. These are handy for term-at-a-time (TAAT) query processing. Two
implementations are available: `SimpleAccumulator` and
`LazyAccumulator`. They both satisfy the `PartialScoreAccumulator`
concept (if using in C++20 mode). For the definition, see
`partial_score_accumulator.hpp`.

`SimpleAccumulator` is a simple wrapper over a `std::vector<float>`,
while `LazyAccumulator` implements some optimizations as described in
`lazy_accumulator.hpp`.
