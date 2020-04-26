#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>

#include "index_scorer.hpp"
namespace pisa {

/// Implements the Okapi BM25 model. k1 and b are both free parameters which
/// alter the weight given to different aspects of the calculation.
/// We adopt the defaults recommended by the following resource - A. Trotman,
/// X-F. Jia, and M. Crane: "Towards an Efficient and Effective Search Engine,"
/// in Proceedings of the SIGIR 2012 Workshop on Open Source Information
/// Retrieval (OSIR), 2012.
template <typename Wand>
struct bm25: public index_scorer<Wand> {
    using index_scorer<Wand>::index_scorer;

    bm25(const Wand& wdata, const float b, const float k1)
        : index_scorer<Wand>(wdata), m_b(b), m_k1(k1)
    {}

    float doc_term_weight(uint64_t freq, float norm_len) const
    {
        auto f = static_cast<float>(freq);
        return f / (f + m_k1 * (1.0F - m_b + m_b * norm_len));
    }

    // IDF (inverse document frequency)
    float query_term_weight(uint64_t df, uint64_t num_docs) const
    {
        auto fdf = static_cast<float>(df);
        float idf = std::log((float(num_docs) - fdf + 0.5F) / (fdf + 0.5F));
        static const float epsilon_score = 1.0E-6;
        return std::max(epsilon_score, idf) * (1.0F + m_k1);
    }

    term_scorer_t term_scorer(uint64_t term_id) const override
    {
        auto term_len = this->m_wdata.term_posting_count(term_id);
        auto term_weight = query_term_weight(term_len, this->m_wdata.num_docs());
        auto s = [&, term_weight](uint32_t doc, uint32_t freq) {
            return term_weight * doc_term_weight(freq, this->m_wdata.norm_len(doc));
        };
        return s;
    }

  private:
    float m_b;
    float m_k1;
};
}  // namespace pisa
