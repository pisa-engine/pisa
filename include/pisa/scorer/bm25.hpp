#pragma once

#include <cstdint>
#include <cmath>
#include <algorithm>
namespace pisa {

    struct bm25 {
        static constexpr float b = 0.5;
        static constexpr float k1 = 1.2;


        static float doc_term_weight(uint64_t freq, float norm_len)
        {
            auto f = static_cast<float>(freq);
            return f / (f + k1 * (1.0f - b + b * norm_len));
        }

        //IDF (inverse document frequency)
        static float query_term_weight(uint64_t freq, uint64_t df, uint64_t num_docs)
        {
            auto f = static_cast<float>(freq);
            auto fdf = static_cast<float>(df);
            float idf = std::log((float(num_docs) - fdf + 0.5f) / (fdf + 0.5f));
            static const float epsilon_score = 1.0E-6;
            return f * std::max(epsilon_score, idf) * (1.0f + k1);
        }
    };

}
