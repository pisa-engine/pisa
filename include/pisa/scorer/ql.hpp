#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
namespace pisa {

template <typename Wand>
struct ql {
    static constexpr float mu = 2000;
    const Wand&            wdata;
    double                 document_len;
    double                 term_id;

    [[nodiscard]] float operator()(uint32_t doc, uint32_t freq) const {
        float numerator =
            freq + mu * wdata.term_count(term_id) / wdata.collection_len();
        float denominator = wdata.doc_len(doc) + mu;
        return std::log(numerator / denominator);
    }
};

}  // namespace pisa
