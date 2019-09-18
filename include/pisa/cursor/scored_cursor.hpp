#pragma once

#include <vector>

#include <boost/preprocessor/cat.hpp>
#include <boost/preprocessor/seq/for_each.hpp>

#include "codec/list.hpp"
#include "query/queries.hpp"

namespace pisa {

template <typename Index, typename TermScorer>
struct scored_cursor {
    using enum_type = typename Index::document_enumerator;
    enum_type docs_enum;
    float q_weight;
    TermScorer scorer;

    ~scored_cursor() {}
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

#define PISA_SCORED_CURSOR_EXTERN(SCORER, INDEX, WAND)                                   \
    extern template scored_cursor<                                                       \
        BOOST_PP_CAT(INDEX, _index),                                                     \
        typename scorer_traits<SCORER<wand_data<WAND>>>::term_scorer>::~scored_cursor(); \
    extern template auto                                                                 \
    make_scored_cursors<BOOST_PP_CAT(INDEX, _index), SCORER<wand_data<WAND>>>(           \
        BOOST_PP_CAT(INDEX, _index) const &, SCORER<wand_data<WAND>> const &, Query);

#define LOOP_BODY(R, DATA, T)                                \
    struct T;                                                \
    PISA_SCORED_CURSOR_EXTERN(bm25, T, wand_data_raw)        \
    PISA_SCORED_CURSOR_EXTERN(dph, T, wand_data_raw)         \
    PISA_SCORED_CURSOR_EXTERN(pl2, T, wand_data_raw)         \
    PISA_SCORED_CURSOR_EXTERN(qld, T, wand_data_raw)         \
    PISA_SCORED_CURSOR_EXTERN(bm25, T, wand_data_compressed) \
    PISA_SCORED_CURSOR_EXTERN(dph, T, wand_data_compressed)  \
    PISA_SCORED_CURSOR_EXTERN(pl2, T, wand_data_compressed)  \
    PISA_SCORED_CURSOR_EXTERN(qld, T, wand_data_compressed)  \
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY

} // namespace pisa
