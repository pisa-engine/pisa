#pragma once

#include <vector>

#include "query.hpp"
#include "scorer/index_scorer.hpp"
#include "util/compiler_attribute.hpp"

namespace pisa {

template <typename Scorer>
auto resolve_term_scorer(Scorer scorer, float weight) -> TermScorer {
    if (weight == 1.0F) {
        // Optimization: no multiplication necessary if weight is 1.0
        return scorer;
    }
    return [scorer, weight](uint32_t doc, uint32_t freq) { return weight * scorer(doc, freq); };
}

template <typename Cursor>
class ScoredCursor {
  public:
    using base_cursor_type = Cursor;

    ScoredCursor(Cursor cursor, TermScorer term_scorer, float weight)
        : m_base_cursor(std::move(cursor)),
          m_weight(weight),
          m_term_scorer(resolve_term_scorer(term_scorer, weight)) {}
    ScoredCursor(ScoredCursor const&) = delete;
    ScoredCursor(ScoredCursor&&) = default;
    ScoredCursor& operator=(ScoredCursor const&) = delete;
    ScoredCursor& operator=(ScoredCursor&&) = default;
    ~ScoredCursor() = default;

    [[nodiscard]] PISA_ALWAYSINLINE auto weight() const noexcept -> float { return m_weight; }
    [[nodiscard]] PISA_ALWAYSINLINE auto docid() const -> std::uint32_t {
        return m_base_cursor.docid();
    }
    [[nodiscard]] PISA_ALWAYSINLINE auto freq() -> std::uint32_t { return m_base_cursor.freq(); }
    [[nodiscard]] PISA_ALWAYSINLINE auto score() -> float { return m_term_scorer(docid(), freq()); }
    void PISA_ALWAYSINLINE next() { m_base_cursor.next(); }
    void PISA_ALWAYSINLINE next_geq(std::uint32_t docid) { m_base_cursor.next_geq(docid); }
    [[nodiscard]] PISA_ALWAYSINLINE auto size() -> std::size_t { return m_base_cursor.size(); }

  private:
    Cursor m_base_cursor;
    float m_weight = 1.0;
    TermScorer m_term_scorer;
};

template <typename Index, typename Scorer>
[[nodiscard]] auto make_scored_cursors(
    Index const& index, Scorer const& scorer, Query const& query, bool weighted = false
) {
    std::vector<ScoredCursor<typename Index::document_enumerator>> cursors;
    cursors.reserve(query.terms().size());
    std::transform(
        query.terms().begin(),
        query.terms().end(),
        std::back_inserter(cursors),
        [&](WeightedTerm const& term) {
            return ScoredCursor<typename Index::document_enumerator>(
                index[term.id], scorer.term_scorer(term.id), weighted ? term.weight : 1.0
            );
        }
    );
    return cursors;
}

}  // namespace pisa
