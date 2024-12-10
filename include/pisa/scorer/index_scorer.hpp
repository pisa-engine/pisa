#pragma once

#include <cstdint>
#include <functional>

namespace pisa {

using TermScorer = std::function<float(uint32_t, uint32_t)>;

/** Index scorer construct scorers for terms in the index. */
class IndexScorer {
  public:
    IndexScorer() = default;
    IndexScorer(const IndexScorer&) = default;
    IndexScorer(IndexScorer&&) noexcept = default;
    IndexScorer& operator=(const IndexScorer&) = delete;
    IndexScorer& operator=(IndexScorer&&) noexcept = delete;
    virtual ~IndexScorer() = default;
    virtual TermScorer term_scorer(std::uint64_t term_id) const = 0;
};

/** Index scorer using WAND metadata for scoring. */
template <typename Wand>
struct WandIndexScorer: public IndexScorer {
  protected:
    const Wand& m_wdata;

  public:
    explicit WandIndexScorer(const Wand& wdata) : m_wdata(wdata) {}
    WandIndexScorer(WandIndexScorer const&) = default;
    WandIndexScorer(WandIndexScorer&&) noexcept = default;
    WandIndexScorer& operator=(WandIndexScorer const&) = delete;
    WandIndexScorer& operator=(WandIndexScorer&&) noexcept = delete;
    virtual ~WandIndexScorer() = default;
};

}  // namespace pisa
