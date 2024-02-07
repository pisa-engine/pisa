#pragma once

#include <algorithm>
#include <cstdint>
#include <utility>

#include "index_scorer.hpp"
#include "linear_quantizer.hpp"

namespace pisa {

template <typename Wand>
struct quantized: public index_scorer<Wand> {
    using index_scorer<Wand>::index_scorer;

    term_scorer_t term_scorer([[maybe_unused]] uint64_t term_id) const {
        return []([[maybe_unused]] uint32_t doc, uint32_t freq) { return freq; };
    }
};

/**
 * Uses internal scorer and quantizer to produce quantized scores.
 *
 * This is not inheriting from `index_scorer` because it returns int scores.
 */
template <typename Wand>
class QuantizingScorer {
  private:
    std::unique_ptr<index_scorer<Wand>> m_scorer;
    LinearQuantizer m_quantizer;

  public:
    QuantizingScorer(std::unique_ptr<index_scorer<Wand>> scorer, LinearQuantizer quantizer)
        : m_scorer(std::move(scorer)), m_quantizer(quantizer) {}

    [[nodiscard]] auto term_scorer(std::uint64_t term_id) const
        -> std::function<std::uint32_t(std::uint32_t, std::uint32_t)> {
        return
            [this, scorer = m_scorer->term_scorer(term_id)](std::uint32_t doc, std::uint32_t freq) {
                return this->m_quantizer(scorer(doc, freq));
            };
    }
};

}  // namespace pisa
