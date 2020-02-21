#pragma once

#include <cstdint>
#include <functional>

#include <gsl/span>
#include <mio/mmap.hpp>

#include "v1/types.hpp"

namespace pisa::v1 {

struct VectorSource {
    std::vector<std::vector<std::byte>> bytes{};
    std::vector<std::vector<std::size_t>> offsets{};
    std::vector<std::vector<std::uint32_t>> sizes{};
};

struct MMapSource {
    MMapSource() = default;
    MMapSource(MMapSource &&) = default;
    MMapSource(MMapSource const &) = default;
    MMapSource &operator=(MMapSource &&) = default;
    MMapSource &operator=(MMapSource const &) = default;
    ~MMapSource() = default;
    std::vector<std::shared_ptr<mio::mmap_source>> file_sources{};
};

} // namespace pisa::v1
