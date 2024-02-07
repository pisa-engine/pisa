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

    term_scorer_t term_scorer(uint64_t term_id) const override {
        float mu = this->m_mu;
        float collection_len = this->m_wdata.collection_len();
        float term_occurrences = this->m_wdata.term_occurrence_count(term_id);
        float term_component = collection_len / (mu * term_occurrences);

        auto s = [this, mu, term_component, term_id](uint32_t doc, uint32_t freq) {
            float doclen = this->m_wdata.doc_len(doc);
            float a = std::log(mu / (doclen + mu));
            float b = std::log1p(freq * term_component);
            return std::max(0.F, a + b);
        };
        return s;
    }

  private:
    float m_mu;
};

}  // namespace pisa
