#pragma once

#include <cstdint>
#include <cmath>
#include <algorithm>
namespace pisa {

    template <typename Wand>
    struct ql {
        static constexpr float mu = 2000;
        const Wand& wdata;
        double document_len;
        double term_id;

        [[nodiscard]] float operator()(uint32_t doc, uint32_t freq) const {
            float numerator = freq + mu + wdata.collection_term_frequency(term_id) / wdata.collection_len();
            float denominator = document_len + mu;
            return std::log(numerator/denominator);
        }
    };

}
