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

template <typename Index>
struct IndexTypeMarker {
    using type = Index;
};

namespace detail {
    inline std::string_view index_name(ef_index const&) noexcept { return "ef"; }
    inline std::string_view index_name(single_index const&) noexcept { return "single"; }
    inline std::string_view index_name(pefuniform_index const&) noexcept { return "pefuniform"; }
    inline std::string_view index_name(pefopt_index const&) noexcept { return "pefopt"; }
    inline std::string_view index_name(block_optpfor_index const&) noexcept
    {
        return "block_optpfor";
    }
    inline std::string_view index_name(block_varintg8iu_index const&) noexcept
    {
        return "block_varintg8iu";
    }
    inline std::string_view index_name(block_streamvbyte_index const&) noexcept
    {
        return "block_streamvbyte";
    }
    inline std::string_view index_name(block_maskedvbyte_index const&) noexcept
    {
        return "block_maskedvbyte";
    }
    inline std::string_view index_name(block_interpolative_index const&) noexcept
    {
        return "block_interpolative";
    }
    inline std::string_view index_name(block_qmx_index const&) noexcept { return "block_qmx"; }
    inline std::string_view index_name(block_varintgb_index const&) noexcept
    {
        return "block_varintgb";
    }
    inline std::string_view index_name(block_simple8b_index const&) noexcept
    {
        return "block_simple8b";
    }
    inline std::string_view index_name(block_simple16_index const&) noexcept
    {
        return "block_simple16";
    }
    inline std::string_view index_name(block_simdbp_index const&) noexcept
    {
        return "block_simdbp";
    }

    template <typename Index>
    struct add_profiling {
        using type = Index;
    };

    template <typename Block>
    struct add_profiling<block_freq_index<Block, false>> {
        using type = block_freq_index<Block, true>;
    };

    template <bool Profile, std::size_t I, typename Fn>
    auto with_alternative(std::string_view index_type, std::string const& path, Fn&& fn) -> bool
    {
        using T = std::variant_alternative_t<I, Index>;

        if (index_type == detail::index_name(std::variant_alternative_t<I, Index>())) {
            if constexpr (Profile) {
                using Index = typename add_profiling<T>::type;
                fn(Index(MemorySource::mapped_file(path)));
            } else {
                fn(T(MemorySource::mapped_file(path)));
            }
            return true;
        }
        return false;
    }

    template <bool Profile, std::size_t I, typename Fn>
    auto with_type_alternative(std::string_view index_type, std::string const& path, Fn&& fn) -> bool
    {
        using T = std::variant_alternative_t<I, Index>;

        if (index_type == detail::index_name(std::variant_alternative_t<I, Index>())) {
            if constexpr (Profile) {
                using Index = typename add_profiling<T>::type;
                fn(IndexTypeMarker<Index>{});
            } else {
                fn(IndexTypeMarker<T>{});
            }
            return true;
        }
        return false;
    }

    template <bool Profile, typename Fn, std::size_t... I>
    auto with_alternatives(
        std::string_view index_type,
        std::string const& path,
        Fn&& fn,
        std::integer_sequence<std::size_t, I...>) -> bool
    {
        return (with_alternative<Profile, I, Fn>(index_type, path, std::forward<Fn>(fn)) && ...);
    }

    template <bool Profile, typename Fn, std::size_t... I>
    auto with_type_alternatives(
        std::string_view index_type,
        std::string const& path,
        Fn&& fn,
        std::integer_sequence<std::size_t, I...>) -> bool
    {
        return (with_type_alternative<Profile, I, Fn>(index_type, path, std::forward<Fn>(fn)) && ...);
    }
};  // namespace detail

class InvalidEncoding {
  public:
    explicit InvalidEncoding(std::string_view encoding)
        : m_message(fmt::format("Invalid encoding: {}", encoding))
    {}
    [[nodiscard]] auto what() const -> char const* { return m_message.c_str(); }

  private:
    std::string m_message;
};

/// Executes function `fn(index)` where `index` is the index loaded from `path` and its type
/// depends on the `encoding` value.
///
/// # Exceptions
/// `InvalidEncoding` is thrown if an invalid `encoding` value was passed.
template <typename Fn>
void with_index(std::string_view encoding, std::string const& path, Fn&& fn)
{
    std::string full_index_type = fmt::format("{}_index", encoding);
    constexpr auto num_types = std::variant_size_v<Index>;
    if (!detail::with_alternatives<false>(
            encoding, path, std::forward<Fn>(fn), std::make_index_sequence<num_types>{})) {
        throw InvalidEncoding(encoding);
    }
}

/// Works similar to `with_index` but sets profiling to `true` in the index type.
///
/// # Exceptions
/// `InvalidEncoding` is thrown if an invalid `encoding` value was passed.
template <typename Fn>
void with_profiling_index(std::string_view encoding, std::string const& path, Fn&& fn)
{
    std::string full_index_type = fmt::format("{}_index", encoding);
    constexpr auto num_types = std::variant_size_v<Index>;
    if (!detail::with_alternatives<true>(
            encoding, path, std::forward<Fn>(fn), std::make_index_sequence<num_types>{})) {
        throw InvalidEncoding(encoding);
    }
}

/// Executes function `fn(marker)` where `marker` is a value of a special marker type that has an
/// inner type `type` declared that is the type of the index associated with `encoding`.
///
/// This function can be used in a rare case when instead of the index, we only need the information
/// about its type, e.g., `fn` can be a function that builds the index, thus no index to load exists
/// yet. The type can be accessed the following way:
///
/// ```
/// with_index_type(encoding, path, [](auto marker){
///     using Index = typename decltype(marker)::type;
/// });
/// ```
///
/// # Exceptions
/// `InvalidEncoding` is thrown if an invalid `encoding` value was passed.
template <typename Fn>
void with_index_type(std::string_view encoding, std::string const& path, Fn&& fn)
{
    std::string full_index_type = fmt::format("{}_index", encoding);
    constexpr auto num_types = std::variant_size_v<Index>;
    if (!detail::with_type_alternatives<true>(
            encoding, path, std::forward<Fn>(fn), std::make_index_sequence<num_types>{})) {
        throw InvalidEncoding(encoding);
    }
}

template <std::size_t I>
void push_encoding(std::vector<std::string>& encodings)
{
    encodings.emplace_back(detail::index_name(std::variant_alternative_t<I, Index>()));
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
