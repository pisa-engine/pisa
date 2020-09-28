#pragma once

#include <typeinfo>

#include "boost/preprocessor/cat.hpp"
#include "boost/preprocessor/seq/for_each.hpp"
#include "boost/preprocessor/stringize.hpp"

#include "codec/block_codecs.hpp"
#include "codec/maskedvbyte.hpp"
#include "codec/qmx.hpp"
#include "codec/simdbp.hpp"
#include "codec/simple16.hpp"
#include "codec/simple8b.hpp"
#include "codec/streamvbyte.hpp"
#include "codec/varintgb.hpp"

#include "binary_freq_collection.hpp"
#include "block_freq_index.hpp"

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

using block_optpfor_index = block_freq_index<pisa::optpfor_block>;
using block_varintg8iu_index = block_freq_index<pisa::varint_G8IU_block>;
using block_streamvbyte_index = block_freq_index<pisa::streamvbyte_block>;
using block_maskedvbyte_index = block_freq_index<pisa::maskedvbyte_block>;
using block_varintgb_index = block_freq_index<pisa::varintgb_block>;
using block_interpolative_index = block_freq_index<pisa::interpolative_block>;
using block_qmx_index = block_freq_index<pisa::qmx_block>;
using block_simple8b_index = block_freq_index<pisa::simple8b_block>;
using block_simple16_index = block_freq_index<pisa::simple16_block>;
using block_simdbp_index = block_freq_index<pisa::simdbp_block>;

constexpr static auto INDEX_TYPES = std::array{
    "ef",
    "single",
    "pefuniform",
    "pefopt",
    "block_optpfor",
    "block_varintg8iu",
    "block_streamvbyte",
    "block_maskedvbyte",
    "block_interpolative",
    "block_qmx",
    "block_varintgb",
    "block_simple8b",
    "block_simple16",
    "block_simdbp",
};

using Index = std::variant<
    ef_index,
    single_index,
    pefuniform_index,
    pefopt_index,
    block_optpfor_index,
    block_varintg8iu_index,
    block_streamvbyte_index,
    block_maskedvbyte_index,
    block_interpolative_index,
    block_qmx_index,
    block_varintgb_index,
    block_simple8b_index,
    block_simple16_index,
    block_simdbp_index>;

class InvalidEncoding {
  public:
    explicit InvalidEncoding(std::string_view encoding)
        : m_message(fmt::format("Invalid encoding: {}", encoding))
    {}
    [[nodiscard]] auto what() const -> char const* { return m_message.c_str(); }

  private:
    std::string m_message;
};

template <std::size_t I, typename Fn>
auto with_alternative(std::string_view index_type, std::string const& path, Fn&& fn) -> bool
{
    using T = std::variant_alternative_t<I, Index>;

    if (index_type == INDEX_TYPES[I]) {
        fn(T(MemorySource::mapped_file(path)));
        return true;
    }
    return false;
}

template <typename Fn, std::size_t... I>
auto with_alternatives(
    std::string_view index_type,
    std::string const& path,
    Fn&& fn,
    std::integer_sequence<std::size_t, I...>) -> bool
{
    return (with_alternative<I, Fn>(index_type, path, std::forward<Fn>(fn)) && ...);
}

template <typename Fn>
void with_index(std::string_view encoding, std::string const& path, Fn&& fn)
{
    std::string full_index_type = fmt::format("{}_index", encoding);
    constexpr auto num_types = std::variant_size_v<Index>;
    if (!with_alternatives(
            encoding, path, std::forward<Fn>(fn), std::make_index_sequence<num_types>{})) {
        throw InvalidEncoding(encoding);
    }
}

template <std::size_t I>
void push_encoding(std::vector<std::string>& encodings)
{
    encodings.push_back(INDEX_TYPES[I]);
}

template <std::size_t... I>
auto encodings(std::integer_sequence<std::size_t, I...>) -> std::vector<std::string>
{
    std::vector<std::string> encodings;
    (push_encoding<I>(encodings), ...);
    return encodings;
}

inline auto encodings() -> std::vector<std::string>
{
    return encodings(std::make_index_sequence<variant_size_v<Index>>{});
}

}  // namespace pisa
