#include "accumulator/lazy_accumulator.hpp"

namespace pisa {

template LazyAccumulator<4, std::uint64_t>::LazyAccumulator(std::size_t);
template void LazyAccumulator<4, std::uint64_t>::init();
template void LazyAccumulator<4, std::uint64_t>::accumulate(std::ptrdiff_t const, float);
template auto LazyAccumulator<4, std::uint64_t>::size() const noexcept -> std::size_t;
template auto LazyAccumulator<4, std::uint64_t>::blocks() noexcept -> std::vector<Block> &;
template auto LazyAccumulator<4, std::uint64_t>::counter() const noexcept -> int;

} // namespace pisa
