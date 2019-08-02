#pragma once

#include "scorer/index_scorer.hpp"
#include "query/queries.hpp"
#include <vector>

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

#define LOOP_BODY(R, DATA, T)                                                                  \
    struct T;                                                                                  \
                                                                                               \
    extern template auto make_block_max_scored_cursors<block_freq_index<T, false>,             \
                                                       wand_data<wand_data_raw>,               \
                                                       bm25<wand_data<wand_data_raw>>>(        \
        block_freq_index<T, false> const &,                                                    \
        wand_data<wand_data_raw> const &,                                                      \
        bm25<wand_data<wand_data_raw>> const &,                                                \
        Query);                                                                                \
    extern template auto make_block_max_scored_cursors<block_freq_index<T, false>,             \
                                                       wand_data<wand_data_raw>,               \
                                                       dph<wand_data<wand_data_raw>>>(         \
        block_freq_index<T, false> const &,                                                    \
        wand_data<wand_data_raw> const &,                                                      \
        dph<wand_data<wand_data_raw>> const &,                                                 \
        Query);                                                                                \
    extern template auto make_block_max_scored_cursors<block_freq_index<T, false>,             \
                                                       wand_data<wand_data_raw>,               \
                                                       pl2<wand_data<wand_data_raw>>>(         \
        block_freq_index<T, false> const &,                                                    \
        wand_data<wand_data_raw> const &,                                                      \
        pl2<wand_data<wand_data_raw>> const &,                                                 \
        Query);                                                                                \
    extern template auto make_block_max_scored_cursors<block_freq_index<T, false>,             \
                                                       wand_data<wand_data_raw>,               \
                                                       qld<wand_data<wand_data_raw>>>(         \
        block_freq_index<T, false> const &,                                                    \
        wand_data<wand_data_raw> const &,                                                      \
        qld<wand_data<wand_data_raw>> const &,                                                 \
        Query);                                                                                \
                                                                                               \
    extern template auto make_block_max_scored_cursors<block_freq_index<T, false>,             \
                                                       wand_data<wand_data_compressed>,        \
                                                       bm25<wand_data<wand_data_compressed>>>( \
        block_freq_index<T, false> const &,                                                    \
        wand_data<wand_data_compressed> const &,                                               \
        bm25<wand_data<wand_data_compressed>> const &,                                         \
        Query);                                                                                \
    extern template auto make_block_max_scored_cursors<block_freq_index<T, false>,             \
                                                       wand_data<wand_data_compressed>,        \
                                                       dph<wand_data<wand_data_compressed>>>(  \
        block_freq_index<T, false> const &,                                                    \
        wand_data<wand_data_compressed> const &,                                               \
        dph<wand_data<wand_data_compressed>> const &,                                          \
        Query);                                                                                \
    extern template auto make_block_max_scored_cursors<block_freq_index<T, false>,             \
                                                       wand_data<wand_data_compressed>,        \
                                                       pl2<wand_data<wand_data_compressed>>>(  \
        block_freq_index<T, false> const &,                                                    \
        wand_data<wand_data_compressed> const &,                                               \
        pl2<wand_data<wand_data_compressed>> const &,                                          \
        Query);                                                                                \
    extern template auto make_block_max_scored_cursors<block_freq_index<T, false>,             \
                                                       wand_data<wand_data_compressed>,        \
                                                       qld<wand_data<wand_data_compressed>>>(  \
        block_freq_index<T, false> const &,                                                    \
        wand_data<wand_data_compressed> const &,                                               \
        qld<wand_data<wand_data_compressed>> const &,                                          \
        Query);                                                                                \
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_BLOCK_CODEC_TYPES);
#undef LOOP_BODY

} // namespace pisa
