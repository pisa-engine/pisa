#pragma once

#include <vector>

#include <boost/algorithm/string.hpp>

#include "codec/list.hpp"
#include "index_types.hpp"
#include "query/queries.hpp"
#include "scorer/index_scorer.hpp"

namespace pisa {

template <typename Index, typename WandType, typename TermScorer>
struct block_max_scored_cursor {
    using enum_type = typename Index::document_enumerator;
    using wdata_enum = typename WandType::wand_data_enumerator;

    enum_type docs_enum;
    wdata_enum w;
    float q_weight;
    TermScorer scorer;
    float max_weight;

    ~block_max_scored_cursor() {}
};

template <typename Index, typename WandType, typename Scorer>
[[nodiscard]] auto make_block_max_scored_cursors(Index const &index,
                                                 WandType const &wdata,
                                                 Scorer const &scorer,
                                                 Query query)
{
    auto terms = query.terms;
    auto query_term_freqs = query_freqs(terms);
    using term_scorer_type = typename scorer_traits<Scorer>::term_scorer;

    std::vector<block_max_scored_cursor<Index, WandType, term_scorer_type>> cursors;
    cursors.reserve(query_term_freqs.size());
    std::transform(
        query_term_freqs.begin(),
        query_term_freqs.end(),
        std::back_inserter(cursors),
        [&](auto &&term) {
            auto list = index[term.first];
            auto w_enum = wdata.getenum(term.first);
            float q_weight = term.second;
            auto max_weight = q_weight * wdata.max_term_weight(term.first);
            return block_max_scored_cursor<Index, WandType, term_scorer_type>{
                std::move(list), w_enum, q_weight, scorer.term_scorer(term.first), max_weight};
        });
    return cursors;
}

template <typename Index, bool Profile>
struct block_freq_index;

#define PISA_BLOCK_MAX_SCORED_CURSOR_EXTERN(SCORER, INDEX, WAND)                                   \
    extern template block_max_scored_cursor<                                                       \
        BOOST_PP_CAT(INDEX, _index),                                                               \
        wand_data<WAND>,                                                                           \
        typename scorer_traits<SCORER<wand_data<WAND>>>::term_scorer>::~block_max_scored_cursor(); \
    extern template auto make_block_max_scored_cursors<BOOST_PP_CAT(INDEX, _index),                \
                                                       wand_data<WAND>,                            \
                                                       SCORER<wand_data<WAND>>>(                   \
        BOOST_PP_CAT(INDEX, _index) const &,                                                       \
        wand_data<WAND> const &,                                                                   \
        SCORER<wand_data<WAND>> const &,                                                           \
        Query);

#define LOOP_BODY(R, DATA, T)                                          \
    struct BOOST_PP_CAT(INDEX, _index);                                \
    PISA_BLOCK_MAX_SCORED_CURSOR_EXTERN(bm25, T, wand_data_raw)        \
    PISA_BLOCK_MAX_SCORED_CURSOR_EXTERN(dph, T, wand_data_raw)         \
    PISA_BLOCK_MAX_SCORED_CURSOR_EXTERN(pl2, T, wand_data_raw)         \
    PISA_BLOCK_MAX_SCORED_CURSOR_EXTERN(qld, T, wand_data_raw)         \
    PISA_BLOCK_MAX_SCORED_CURSOR_EXTERN(bm25, T, wand_data_compressed) \
    PISA_BLOCK_MAX_SCORED_CURSOR_EXTERN(dph, T, wand_data_compressed)  \
    PISA_BLOCK_MAX_SCORED_CURSOR_EXTERN(pl2, T, wand_data_compressed)  \
    PISA_BLOCK_MAX_SCORED_CURSOR_EXTERN(qld, T, wand_data_compressed)  \
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY

} // namespace pisa
