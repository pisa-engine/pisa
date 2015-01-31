#pragma once

#include <cmath>

namespace ds2i {

    struct bm25 {
        static constexpr float b = 0.5;
        static constexpr float k1 = 1.2;

        static float doc_term_weight(uint64_t freq, float norm_len)
        {
            float f = (float)freq;
            return f / (f + k1 * (1.0f - b + b * norm_len));
        }

        static float query_term_weight(uint64_t freq, uint64_t df, uint64_t num_docs)
        {
            float f = (float)freq;
            float fdf = (float)df;
            float idf = std::log((float(num_docs) - fdf + 0.5f) / (fdf + 0.5f));
            static const float epsilon_score = 1.0E-6;
            return f * std::max(epsilon_score, idf) * (1.0f + k1);
        }
    };

}
