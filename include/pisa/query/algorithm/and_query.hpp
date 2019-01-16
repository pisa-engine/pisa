#pragma once

namespace pisa {

template <bool with_freqs>
struct and_query {

    template <typename Index>
    uint64_t operator()(Index const &index, term_id_vec terms) const {
        if (terms.empty())
            return 0;
        remove_duplicate_terms(terms);

        typedef typename Index::document_enumerator enum_type;
        std::vector<enum_type>                      enums;
        enums.reserve(terms.size());

        for (auto term : terms) {
            enums.push_back(index[term]);
        }

        // sort by increasing frequency
        std::sort(enums.begin(), enums.end(), [](enum_type const &lhs, enum_type const &rhs) {
            return lhs.size() < rhs.size();
        });

        uint64_t results   = 0;
        uint64_t candidate = enums[0].docid();
        size_t   i         = 1;
        while (candidate < index.num_docs()) {
            for (; i < enums.size(); ++i) {
                enums[i].next_geq(candidate);
                if (enums[i].docid() != candidate) {
                    candidate = enums[i].docid();
                    i         = 0;
                    break;
                }
            }

            if (i == enums.size()) {
                results += 1;

                if (with_freqs) {
                    for (i = 0; i < enums.size(); ++i) {
                        do_not_optimize_away(enums[i].freq());
                    }
                }
                enums[0].next();
                candidate = enums[0].docid();
                i         = 1;
            }
        }
        return results;
    }
};

} // namespace pisa