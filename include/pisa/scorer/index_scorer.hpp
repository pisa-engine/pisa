#pragma once

#include <cstdint>
#include <functional>

namespace pisa {

using term_scorer_t = std::function<float(uint32_t, uint32_t)>;
using TermScorer = std::function<float(uint32_t, uint32_t)>;

class IndexScorer {
  public:
    virtual TermScorer term_scorer(std::uint64_t term_id) const = 0;
};

template <typename Wand>
struct index_scorer: IndexScorer {
  protected:
    const Wand& m_wdata;

  public:
    explicit index_scorer(const Wand& wdata) : m_wdata(wdata) {}
    index_scorer(index_scorer const&) = default;
    index_scorer(index_scorer&&) noexcept = default;
    index_scorer& operator=(index_scorer const&) = delete;
    index_scorer& operator=(index_scorer&&) noexcept = delete;
    virtual ~index_scorer() = default;
};

}  // namespace pisa
