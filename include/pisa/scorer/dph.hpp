#pragma once

#define _USE_MATH_DEFINES

#include <cmath>
#include <cstdint>

#include "index_scorer.hpp"

namespace pisa {

/// Implements the DPH model. This model is parameter free.
/// See the following resource for further information - G. Amati, E. Ambrosi,
/// M Bianchi, C Gaibisso, and G Gambosi: "FUB, IASI-CNR and University of Tor
/// Vergata at TREC 2007 Blog Track," in Proceedings of the 16th Text REtrieval
/// Conference (TREC), 2007.
template <typename Wand>
struct dph: public WandIndexScorer<Wand> {
    using WandIndexScorer<Wand>::WandIndexScorer;

    TermScorer term_scorer(uint64_t term_id) const override {
        auto s = [&, term_id](uint32_t doc, uint32_t freq) {
            float f = (float)freq / this->m_wdata.doc_len(doc);
            float norm = (1.F - f) * (1.F - f) / (freq + 1.F);
            return norm
                * (freq
                       * std::log2(
                           (freq * this->m_wdata.avg_len() / this->m_wdata.doc_len(doc))
                           * ((float)this->m_wdata.num_docs()
                              / this->m_wdata.term_occurrence_count(term_id))
                       )
                   + .5F * std::log2(2.F * M_PI * freq * (1.F - f)));
        };
        return s;
    }
};

}  // namespace pisa
