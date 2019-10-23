#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
namespace pisa {

template <typename WData>
struct bm25 {
    static constexpr float b = 0.4;
    static constexpr float k1 = 0.9;

    bm25(WData const &wdata) : m_wdata(wdata) {}

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

    auto term_scorer(uint64_t term_id) const
    {
        auto term_len = m_wdata.term_posting_count(term_id);
        auto s = [&, term_len](uint32_t doc, uint32_t freq) {
            return query_term_weight(term_len, m_wdata.num_docs())
                   * doc_term_weight(freq, m_wdata.norm_len(doc));
        };
        return s;
    }

    WData const &m_wdata;
};

} // namespace pisa
