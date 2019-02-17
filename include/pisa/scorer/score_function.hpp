#pragma once

template <typename Scorer, typename Wand>
struct Score_Function {
    float query_weight;
    const Wand&  wdata;

    [[nodiscard]] auto operator()(uint32_t doc, uint32_t freq) const -> float {
        return query_weight * Scorer::doc_term_weight(freq, wdata.norm_len(doc));
    }
};