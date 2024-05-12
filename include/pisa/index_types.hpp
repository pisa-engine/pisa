#pragma once

#include "block_inverted_index.hpp"
#include "codec/block_codec_registry.hpp"
#include "freq_index.hpp"
#include "sequence/partitioned_sequence.hpp"
#include "sequence/positive_sequence.hpp"
#include "sequence/uniform_partitioned_sequence.hpp"

namespace pisa {

using ef_index = freq_index<compact_elias_fano, positive_sequence<strict_elias_fano>>;

using single_index = freq_index<indexed_sequence, positive_sequence<>>;

using pefuniform_index =
    freq_index<uniform_partitioned_sequence<>, positive_sequence<uniform_partitioned_sequence<strict_sequence>>>;

using pefopt_index =
    freq_index<partitioned_sequence<>, positive_sequence<partitioned_sequence<strict_sequence>>>;

template <typename Fn>
void run_for_index(std::string_view encoding, MemorySource source, Fn&& fn) {
    if (encoding == "ef") {
        fn(ef_index(std::move(source)));
    } else if (encoding == "single") {
        fn(single_index(std::move(source)));
    } else if (encoding == "pefuniform") {
        fn(pefuniform_index(std::move(source)));
    } else if (encoding == "pefopt") {
        fn(pefopt_index(std::move(source)));
    } else if (encoding.rfind("block_", 0) == 0) {
        fn(BlockInvertedIndex(std::move(source), get_block_codec(encoding)));
    } else {
        throw std::invalid_argument(fmt::format("invalid encoding: {}", encoding));
    }
}

template <typename Type>
struct IndexTraits {
    using type = Type;
};

template <typename Fn>
void resolve_freq_index_type(std::string_view encoding, Fn&& fn) {
    if (encoding == "ef") {
        fn(IndexTraits<ef_index>{});
    } else if (encoding == "single") {
        fn(IndexTraits<single_index>{});
    } else if (encoding == "pefuniform") {
        fn(IndexTraits<pefuniform_index>{});
    } else if (encoding == "pefopt") {
        fn(IndexTraits<pefopt_index>{});
    } else {
        throw std::invalid_argument(fmt::format("invalid encoding: {}", encoding));
    }
}

}  // namespace pisa
