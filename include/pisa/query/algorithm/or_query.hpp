#pragma once

#include <vector>
#include "query/queries.hpp"

namespace pisa {

template <typename Index, bool with_freqs>
struct or_query {

    or_query(Index const &index) : m_index(index) {}

    uint64_t operator()(term_id_vec terms) const {
        if (terms.empty())
            return 0;
        remove_duplicate_terms(terms);

        typedef typename Index::document_enumerator enum_type;
        std::vector<enum_type>                      enums;
        enums.reserve(terms.size());

        for (auto term : terms) {
            enums.push_back(m_index[term]);
        }

        uint64_t results = 0;
        uint64_t cur_doc = std::min_element(enums.begin(),
                                            enums.end(),
                                            [](enum_type const &lhs, enum_type const &rhs) {
                                                return lhs.docid() < rhs.docid();
                                            })
                               ->docid();

        while (cur_doc < m_index.num_docs()) {
            results += 1;
            uint64_t next_doc = m_index.num_docs();
            for (size_t i = 0; i < enums.size(); ++i) {
                if (enums[i].docid() == cur_doc) {
                    if (with_freqs) {
                        do_not_optimize_away(enums[i].freq());
                    }
                    enums[i].next();
                }
                if (enums[i].docid() < next_doc) {
                    next_doc = enums[i].docid();
                }
            }

            cur_doc = next_doc;
        }

        return results;
    }

   private:
    Index const &m_index;
};

} // namespace pisa