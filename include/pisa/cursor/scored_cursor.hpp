#pragma once

#include <vector>

#include <boost/preprocessor/cat.hpp>
#include <boost/preprocessor/seq/for_each.hpp>

#include "codec/list.hpp"
#include "query/queries.hpp"
#include "scorer/bm25.hpp"
#include "scorer/score_function.hpp"

namespace pisa {

template <typename Index, typename Scorer>
struct scored_cursor {
    using enum_type = typename Index::document_enumerator;
    enum_type docs_enum;
    float q_weight;
    Scorer scorer;
};

template <typename Index, typename WandType>
[[nodiscard]] auto make_scored_cursors(Index const &index, WandType const &wdata, Query query)
{
    auto terms = query.terms;
    auto query_term_freqs = query_freqs(terms);
    using scorer_type = bm25;
    using Scorer = Score_Function<scorer_type, WandType>;

    std::vector<scored_cursor<Index, Scorer>> cursors;
    cursors.reserve(query_term_freqs.size());
    std::transform(
        query_term_freqs.begin(),
        query_term_freqs.end(),
        std::back_inserter(cursors),
        [&](auto &&term) {
            auto list = index[term.first];
            auto q_weight =
                scorer_type::query_term_weight(term.second, list.size(), index.num_docs());
            return scored_cursor<Index, Scorer>{std::move(list), q_weight, {q_weight, wdata}};
        });
    return cursors;
}

struct bm25;
template <typename Scorer>
struct wand_data_raw;
#define LOOP_BODY(R, DATA, T)                                                              \
    struct T;                                                                              \
    template <typename Index, bool Profile>                                                \
    struct block_freq_index;                                                               \
    template <typename Scorer, typename block_wand_type>                                   \
    struct wand_data;                                                                             \
    extern template auto                                                                   \
    make_scored_cursors<block_freq_index<T, false>, wand_data<bm25, wand_data_raw<bm25>>>( \
        block_freq_index<T, false> const &, wand_data<bm25, wand_data_raw<bm25>> const &, Query);
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_BLOCK_CODEC_TYPES);
#undef LOOP_BODY

} // namespace pisa
