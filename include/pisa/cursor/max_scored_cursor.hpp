#pragma once

#include "cursor/scored_cursor.hpp"

//#include "query/queries.hpp"
//#include "scorer/index_scorer.hpp"
//#include "wand_data.hpp"
//#include <vector>
//
// namespace pisa {
//
//// template <typename Index>
//// struct max_scored_cursor {
////    using enum_type = typename Index::document_enumerator;
////    max_scored_cursor() = delete;
////    enum_type docs_enum;
////    float q_weight;
////    term_scorer_t scorer;
////    float max_weight;
////};
//
// template <typename Cursor>
// class MaxScoredCursor {
//  public:
//    using base_cursor_type = Cursor;
//
//    MaxScoredCursor(Cursor cursor, TermScorer term_scorer, float query_weight, float max_score)
//        : m_base_cursor(std::move(cursor)),
//          m_term_scorer(std::move(term_scorer)),
//          m_query_weight(query_weight),
//          m_max_score(max_score)
//    {}
//    MaxScoredCursor(MaxScoredCursor const&) = delete;
//    MaxScoredCursor(MaxScoredCursor&&) = default;
//    MaxScoredCursor& operator=(MaxScoredCursor const&) = delete;
//    MaxScoredCursor& operator=(MaxScoredCursor&&) = default;
//    ~MaxScoredCursor() = default;
//
//    [[nodiscard]] auto query_weight() const noexcept -> float { return m_query_weight; }
//    [[nodiscard]] auto docid() const -> std::uint32_t { return m_base_cursor.docid(); }
//    [[nodiscard]] auto freq() -> std::uint32_t { return m_base_cursor.freq(); }
//    [[nodiscard]] auto score() -> float { return m_term_scorer(docid(), freq()); }
//    [[nodiscard]] auto max_score() const noexcept -> float { return m_max_score; }
//    void next() { m_base_cursor.next(); }
//    void next_geq(std::uint32_t docid) { m_base_cursor.next_geq(docid); }
//    [[nodiscard]] auto size() -> std::size_t { return m_base_cursor.size(); }
//
//  private:
//    Cursor m_base_cursor;
//    TermScorer m_term_scorer;
//    float m_max_score;
//    float m_query_weight = 1.0;
//};
//
// template <typename Index, typename WandType, typename Scorer>
//[[nodiscard]] auto
// make_max_scored_cursors(Index const& index, WandType const& wdata, Scorer const& scorer, Query
// query)
//{
//    auto terms = query.terms;
//    auto query_term_freqs = query_freqs(terms);
//
//    std::vector<MaxScoredCursor<typename Index::document_enumerator>> cursors;
//    cursors.reserve(query_term_freqs.size());
//    std::transform(
//        query_term_freqs.begin(), query_term_freqs.end(), std::back_inserter(cursors), [&](auto&&
//        term) {
//            float query_weight = term.second;
//            auto max_weight = query_weight * wdata.max_term_weight(term.first);
//            return MaxScoredCursor<typename Index::document_enumerator>(
//                index[term.first], scorer.term_scorer(term.first), query_weight, max_weight);
//        });
//    return cursors;
//}
//
//}  // namespace pisa
