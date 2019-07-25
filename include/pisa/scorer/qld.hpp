#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>

#include "index_scorer.hpp"

namespace pisa {

template <typename Wand>
struct qld : public index_scorer<Wand> {
    static constexpr float mu = 1000;

    using index_scorer<Wand>::index_scorer;

    auto term_scorer(uint64_t term_id) const
    {
        auto s = [&, term_id](uint32_t doc, uint32_t freq) {
            float numerator = 1
                              + freq
                                    / (this->mu
                                       * ((float)this->m_wdata.term_occurrence_count(term_id)
                                          / this->m_wdata.collection_len()));
            float denominator = this->mu / (this->m_wdata.doc_len(doc) + this->mu);
            return std::log(numerator) + std::log(denominator);
        };
        return s;
    }
};

template <typename Wand>
struct scorer_traits<qld<Wand>> {
    using term_scorer = decltype(std::declval<qld<Wand>>().term_scorer(0));
};

} // namespace pisa
