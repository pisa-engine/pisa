#pragma once

#define _USE_MATH_DEFINES

#include <cmath>

#include <algorithm>
#include <cmath>
#include <cstdint>

#include "index_scorer.hpp"

namespace pisa {

/// Implements the PL2 model. c is a free parameter.
/// See the following resource for further information -
/// G. Amati: "Probabalistic models for information retrieval based on
/// divergence from randomness." PhD Thesis, University of Glasgow, 2003.
template <typename Wand>
struct pl2: public index_scorer<Wand> {
    using index_scorer<Wand>::index_scorer;

    pl2(const Wand& wdata, const float c) : index_scorer<Wand>(wdata), m_c(c) {}

    term_scorer_t term_scorer(uint64_t term_id) const override
    {
        auto s = [&, term_id](uint32_t doc, uint32_t freq) {
            float tfn =
                freq * std::log2(1.f + (m_c * this->m_wdata.avg_len()) / this->m_wdata.doc_len(doc));
            float norm = 1.f / (tfn + 1.f);
            float f = (1.f * this->m_wdata.term_occurrence_count(term_id))
                / (1.f * this->m_wdata.num_docs());
            float e = std::log(1 / 2.f);
            return norm
                * (tfn * std::log2(1.f / f) + f * e + 0.5f * std::log2(2 * M_PI * tfn)
                   + tfn * (std::log2(tfn) - e));
        };
        return s;
    }

  private:
    float m_c;
};

}  // namespace pisa
