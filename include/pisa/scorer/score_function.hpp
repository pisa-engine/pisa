#pragma once
#include <cstdint>

template <typename Scorer, typename Wand>
struct Score_Function {
    float       query_weight;
    const Wand& wdata;

    [[nodiscard]] float operator()(uint32_t doc, uint32_t freq) const {
        return query_weight * Scorer::doc_term_weight(freq, wdata.doc_len(doc)/wdata.avg_len());
    }
};