#pragma once

#include <vector>

namespace pisa::v1 {

/// **Note**: This currently works only for pair cursor with single-term lookup cursors.
/// This callable transforms a cursor by performing lookups to the current document
/// in the given lookup cursors, and then adding the scores that were found.
/// It uses the same short-circuiting rules before each lookup as `UnionLookupJoin`.
template <typename LookupCursor, typename AboveThresholdFn, typename Inspector = void>
struct LookupTransform {

    LookupTransform(std::vector<LookupCursor> lookup_cursors,
                    float lookup_cursors_upper_bound,
                    AboveThresholdFn above_threshold,
                    Inspector* inspect = nullptr)
        : m_lookup_cursors(std::move(lookup_cursors)),
          m_lookup_cursors_upper_bound(lookup_cursors_upper_bound),
          m_above_threshold(std::move(above_threshold)),
          m_inspect(inspect)
    {
    }

    template <typename Cursor>
    auto operator()(Cursor& cursor)
    {
        auto docid = cursor.value();
        auto scores = cursor.payload();
        float score = std::get<0>(scores) + std::get<1>(scores);
        auto upper_bound = score + m_lookup_cursors_upper_bound;
        for (auto&& lookup_cursor : m_lookup_cursors) {
            //if (docid == 2288) {
            //    std::cout << fmt::format("[checking] doc: {}\tbound: {}\n", docid, upper_bound);
            //}
            if (not m_above_threshold(upper_bound)) {
                return score;
            }
            lookup_cursor.advance_to_geq(docid);
            //std::cout << fmt::format("[b] doc: {}\tbound: {}\n", docid, upper_bound);
            if constexpr (not std::is_void_v<Inspector>) {
                m_inspect->lookup();
            }
            if (PISA_UNLIKELY(lookup_cursor.value() == docid)) {
                auto partial_score = lookup_cursor.payload();
                score += partial_score;
                upper_bound += partial_score;
            }
            upper_bound -= lookup_cursor.max_score();
        }
        return score;
    }

   private:
    std::vector<LookupCursor> m_lookup_cursors;
    float m_lookup_cursors_upper_bound;
    AboveThresholdFn m_above_threshold;
    Inspector* m_inspect;
};

} // namespace pisa::v1
