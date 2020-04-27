#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>

#include "index_scorer.hpp"

namespace pisa {

/// Implements the Query Liklihood model with Dirichlet smoothing.
/// This model has a smoothing parameter, mu.
/// See the following resources for further information - J. M. Ponte, and
/// W. B. Croft: "A Language Modeling Approach to Information Retrieval," in
/// Proceedings of SIGIR, 1998.
/// Also see: C. Zhai and J. Lafferty: "A Study of Smoothing Methods for
/// Language Models Applied to Ad Hoc Information Retrieval," in Proceedings of
/// SIGIR, 2001.
template <typename Wand>
struct qld: public index_scorer<Wand> {
    using index_scorer<Wand>::index_scorer;

    qld(const Wand& wdata, const float mu) : index_scorer<Wand>(wdata), m_mu(mu) {}

    term_scorer_t term_scorer(uint64_t term_id) const override
    {
        auto s = [&, term_id](uint32_t doc, uint32_t freq) {
            float numerator = 1
                + freq
                    / (this->m_mu
                       * ((float)this->m_wdata.term_occurrence_count(term_id)
                          / this->m_wdata.collection_len()));
            float denominator = this->m_mu / (this->m_wdata.doc_len(doc) + this->m_mu);
            return std::max(0.f, std::log(numerator) + std::log(denominator));
        };
        return s;
    }

  private:
    float m_mu;
};

}  // namespace pisa
