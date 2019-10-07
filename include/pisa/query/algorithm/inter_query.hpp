#pragma once

/* #include <vector> */

#include <gsl/span>

#include "cursor/union.hpp"
#include "topk_queue.hpp"

namespace pisa {

struct IntersectionQuery {

    IntersectionQuery(std::uint64_t k) : m_topk(k) {}

    template <typename Cursor>
    uint64_t operator()(gsl::span<Cursor> intersections,
                        gsl::span<Cursor> lookup_cursors,
                        std::uint64_t max_docid);

    std::vector<std::pair<float, std::uint64_t>> const &topk() const { return m_topk.topk(); }

   private:
    TopKQueue m_topk;
};

template <typename Cursor>
uint64_t IntersectionQuery::operator()(gsl::span<Cursor> intersections,
                                       gsl::span<Cursor> lookup_cursors,
                                       std::uint64_t max_docid)
{
    m_topk.clear();

    CursorUnion essential(
        gsl::make_span(intersections), max_docid, float(0), [](float acc, auto &cursor) {
            return acc + cursor.scorer(cursor.docid(), cursor.freq());
        });

    std::uint32_t docid;
    while ((docid = essential.docid()) < max_docid) {
        float score = essential.payload();
        for (auto &cursor : lookup_cursors) {
            cursor.next_geq(docid);
            if (cursor.docid() == docid) {
                score += cursor.scorer(cursor.docid(), cursor.freq());
            }
        }
        m_topk.insert(score, docid);
    }

    m_topk.finalize();
    return m_topk.topk().size();
}

} // namespace pisa
