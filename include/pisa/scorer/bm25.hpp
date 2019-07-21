#pragma once

#include <cstdint>
#include <cmath>
#include <algorithm>

#include "index_scorer.hpp"
namespace pisa {

template <typename Wand>
struct bm25 : public index_scorer<Wand> {
    static constexpr float b = 0.4;
    static constexpr float k1 = 0.9;

    using index_scorer<Wand>::index_scorer;

    static float doc_term_weight(uint64_t freq, float norm_len)
    {
        float f = (float)freq;
        return f / (f + k1 * (1.0f - b + b * norm_len));
    }

    // IDF (inverse document frequency)
    static float query_term_weight(uint64_t df, uint64_t num_docs)
    {
        float fdf = (float)df;
        float idf = std::log((float(num_docs) - fdf + 0.5f) / (fdf + 0.5f));
        static const float epsilon_score = 1.0E-6;
        return std::max(epsilon_score, idf) * (1.0f + k1);
    }

    std::function<float(uint32_t, uint32_t)> term_scorer(uint64_t term_id) const override
    {
        auto term_len = this->m_wdata.term_posting_count(term_id);
        auto s = [&, term_len](uint32_t doc, uint32_t freq) {
            return query_term_weight(term_len, this->m_wdata.num_docs())
                   * doc_term_weight(freq, this->m_wdata.norm_len(doc));
        };
        return s;
    }
};
}
