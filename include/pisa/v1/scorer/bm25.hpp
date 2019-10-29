#pragma once

#include <cmath>
#include <type_traits>
#include <utility>

#include <gsl/gsl_assert>
#include <gsl/span>

#include "v1/types.hpp"

namespace pisa::v1 {

template <typename Index>
struct BM25 {
    static constexpr float b = 0.4;
    static constexpr float k1 = 0.9;

    explicit BM25(Index const &index) : m_index(index) {}

    static float doc_term_weight(uint64_t freq, float norm_len)
    {
        auto f = static_cast<float>(freq);
        return f / (f + k1 * (1.0F - b + b * norm_len));
    }

    static float query_term_weight(uint64_t df, uint64_t num_docs)
    {
        auto fdf = static_cast<float>(df);
        float idf = std::log((float(num_docs) - fdf + 0.5F) / (fdf + 0.5F));
        static const float epsilon_score = 1.0E-6;
        return std::max(epsilon_score, idf) * (1.0F + k1);
    }

    auto term_scorer(TermId term_id) const
    {
        auto term_weight =
            query_term_weight(m_index.term_posting_count(term_id), m_index.num_documents());
        return [this, term_weight](uint32_t doc, uint32_t freq) {
            return term_weight
                   * doc_term_weight(freq, this->m_index.normalized_document_length(doc));
        };
    }

   private:
    Index const &m_index;
};

} // namespace pisa::v1
