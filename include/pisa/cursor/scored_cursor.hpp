#pragma once

#include <vector>

#include <boost/preprocessor/cat.hpp>
#include <boost/preprocessor/seq/for_each.hpp>

#include "codec/list.hpp"
#include "query/queries.hpp"
#include "scorer/index_scorer.hpp"

namespace pisa {

template <typename Index, typename TermScorer>
struct scored_cursor {
    using enum_type = typename Index::document_enumerator;
    enum_type docs_enum;
    float q_weight;
    TermScorer scorer;
};

template <typename Index, typename Scorer>
[[nodiscard]] auto make_scored_cursors(Index const &index, Scorer const &scorer, Query query)
{
    auto terms = query.terms;
    auto query_term_freqs = query_freqs(terms);
    using term_scorer_type = typename scorer_traits<Scorer>::term_scorer;

    std::vector<scored_cursor<Index, term_scorer_type>> cursors;
    cursors.reserve(query_term_freqs.size());
    std::transform(
        query_term_freqs.begin(),
        query_term_freqs.end(),
        std::back_inserter(cursors),
        [&](auto &&term) {
            auto list = index[term.first];
            float q_weight = term.second;
            return scored_cursor<Index, term_scorer_type>{
                std::move(list), q_weight, scorer.term_scorer(term.first)};
        });
    return cursors;
}

template <typename Index, bool Profile>
struct block_freq_index;

#define LOOP_BODY(R, DATA, T)                                                                      \
    struct T;                                                                                      \
                                                                                                   \
    extern template auto                                                                           \
    make_scored_cursors<block_freq_index<T, false>, bm25<wand_data<wand_data_raw>>>(               \
        block_freq_index<T, false> const &, bm25<wand_data<wand_data_raw>> const &, Query);        \
    extern template auto                                                                           \
    make_scored_cursors<block_freq_index<T, false>, dph<wand_data<wand_data_raw>>>(                \
        block_freq_index<T, false> const &, dph<wand_data<wand_data_raw>> const &, Query);         \
    extern template auto                                                                           \
    make_scored_cursors<block_freq_index<T, false>, pl2<wand_data<wand_data_raw>>>(                \
        block_freq_index<T, false> const &, pl2<wand_data<wand_data_raw>> const &, Query);         \
    extern template auto                                                                           \
    make_scored_cursors<block_freq_index<T, false>, qld<wand_data<wand_data_raw>>>(                \
        block_freq_index<T, false> const &, qld<wand_data<wand_data_raw>> const &, Query);         \
                                                                                                   \
    extern template auto                                                                           \
    make_scored_cursors<block_freq_index<T, false>, bm25<wand_data<wand_data_compressed>>>(        \
        block_freq_index<T, false> const &, bm25<wand_data<wand_data_compressed>> const &, Query); \
    extern template auto                                                                           \
    make_scored_cursors<block_freq_index<T, false>, dph<wand_data<wand_data_compressed>>>(         \
        block_freq_index<T, false> const &, dph<wand_data<wand_data_compressed>> const &, Query);  \
    extern template auto                                                                           \
    make_scored_cursors<block_freq_index<T, false>, pl2<wand_data<wand_data_compressed>>>(         \
        block_freq_index<T, false> const &, pl2<wand_data<wand_data_compressed>> const &, Query);  \
    extern template auto                                                                           \
    make_scored_cursors<block_freq_index<T, false>, qld<wand_data<wand_data_compressed>>>(         \
        block_freq_index<T, false> const &, qld<wand_data<wand_data_compressed>> const &, Query);  \
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_BLOCK_CODEC_TYPES);
#undef LOOP_BODY

} // namespace pisa
