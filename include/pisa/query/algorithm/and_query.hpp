#pragma once

#include <vector>
#include "query/queries.hpp"

namespace pisa {

template <typename Index, bool with_freqs>
struct and_query {

    and_query(Index const &index, uint64_t max_docid) : m_index(index), m_max_docid(max_docid) {}

    template<typename Cursor>
    uint64_t operator()(std::vector<Cursor> &&cursors) const {
        if (cursors.empty())
            return 0;


        // sort by increasing frequency
        std::sort(cursors.begin(), cursors.end(), [](Cursor const &lhs, Cursor const &rhs) {
            return lhs.size() < rhs.size();
        });

        uint64_t results   = 0;
        uint64_t candidate = cursors[0].docid();
        size_t   i         = 1;
        while (candidate < m_max_docid) {
            for (; i < cursors.size(); ++i) {
                cursors[i].next_geq(candidate);
                if (cursors[i].docid() != candidate) {
                    candidate = cursors[i].docid();
                    i         = 0;
                    break;
                }
            }

            if (i == cursors.size()) {
                results += 1;

                if (with_freqs) {
                    for (i = 0; i < cursors.size(); ++i) {
                        do_not_optimize_away(cursors[i].freq());
                    }
                }
                cursors[0].next();
                candidate = cursors[0].docid();
                i         = 1;
            }
        }
        return results;
    }

   private:
    Index const &m_index;
    uint64_t m_max_docid;
};

} // namespace pisa