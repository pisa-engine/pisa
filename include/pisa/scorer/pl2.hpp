#pragma once

#define _USE_MATH_DEFINES

#include <cmath>

#include <cmath>
#include <cstdint>

#include "index_scorer.hpp"

namespace pisa {

/// Implements the PL2 model. c is a free parameter.
/// See the following resource for further information -
/// G. Amati: "Probabalistic models for information retrieval based on
/// divergence from randomness." PhD Thesis, University of Glasgow, 2003.
template <typename Wand>
struct pl2: public WandIndexScorer<Wand> {
    using WandIndexScorer<Wand>::WandIndexScorer;

    pl2(const Wand& wdata, const float c) : WandIndexScorer<Wand>(wdata), m_c(c) {}

    TermScorer term_scorer(uint64_t term_id) const override {
        auto s = [&, term_id](uint32_t doc, uint32_t freq) {
            float tfn =
                freq * std::log2(1.F + (m_c * this->m_wdata.avg_len()) / this->m_wdata.doc_len(doc));
            float norm = 1.F / (tfn + 1.F);
            float f = (1.F * this->m_wdata.term_occurrence_count(term_id))
                / (1.F * this->m_wdata.num_docs());
            float e = std::log(1 / 2.F);
            return norm
                * (tfn * std::log2(1.F / f) + f * e + 0.5F * std::log2(2 * M_PI * tfn)
                   + tfn * (std::log2(tfn) - e));
        };
        return s;
    }

  private:
    float m_c;
};

}  // namespace pisa
