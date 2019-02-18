#pragma once

#include <vector>
#include "query/queries.hpp"

namespace pisa {

template <bool with_freqs>
struct or_query {

    or_query(uint64_t max_docid) :   m_max_docid(max_docid) {}

    template<typename Cursor>
    uint64_t operator()(std::vector<Cursor> &&cursors) const {
        if (cursors.empty())
            return 0;

        uint64_t results = 0;
        uint64_t cur_doc = std::min_element(cursors.begin(),
                                            cursors.end(),
                                            [](Cursor const &lhs, Cursor const &rhs) {
                                                return lhs.docid() < rhs.docid();
                                            })
                               ->docid();

        while (cur_doc < m_max_docid) {
            results += 1;
            uint64_t next_doc = m_max_docid;
            for (size_t i = 0; i < cursors.size(); ++i) {
                if (cursors[i].docid() == cur_doc) {
                    if (with_freqs) {
                        do_not_optimize_away(cursors[i].freq());
                    }
                    cursors[i].next();
                }
                if (cursors[i].docid() < next_doc) {
                    next_doc = cursors[i].docid();
                }
            }

            cur_doc = next_doc;
        }

        return results;
    }

   private:
    uint64_t m_max_docid;
};

} // namespace pisa