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
        auto f = static_cast<float>(freq);
        return f / (f + k1 * (1.0F - b + b * norm_len));
    }

    // IDF (inverse document frequency)
    static float query_term_weight(uint64_t df, uint64_t num_docs)
    {
        auto fdf = static_cast<float>(df);
        float idf = std::log((float(num_docs) - fdf + 0.5F) / (fdf + 0.5F));
        static const float epsilon_score = 1.0E-6;
        return std::max(epsilon_score, idf) * (1.0F + k1);
    }

    auto term_scorer(uint64_t term_id) const
    {
        auto term_weight =
            query_term_weight(m_wdata.term_posting_count(term_id), m_wdata.num_docs());
        return [&, term_weight](uint32_t doc, uint32_t freq) {
            return term_weight * doc_term_weight(freq, m_wdata.norm_len(doc));
        };
    }

    WData const &m_wdata;
};

} // namespace pisa
