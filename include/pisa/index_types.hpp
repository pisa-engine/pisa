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

using profiling_block_optpfor_index = block_freq_index<pisa::optpfor_block, true>;
using profiling_block_varintg8iu_index = block_freq_index<pisa::varint_G8IU_block, true>;
using profiling_block_streamvbyte_index = block_freq_index<pisa::streamvbyte_block, true>;
using profiling_block_maskedvbyte_index = block_freq_index<pisa::maskedvbyte_block, true>;
using profiling_block_varintgb_index = block_freq_index<pisa::varintgb_block, true>;
using profiling_block_interpolative_index = block_freq_index<pisa::interpolative_block, true>;
using profiling_block_qmx_index = block_freq_index<pisa::qmx_block, true>;
using profiling_block_simple8b_index = block_freq_index<pisa::simple8b_block, true>;
using profiling_block_simple16_index = block_freq_index<pisa::simple16_block, true>;
using profiling_block_simdbp_index = block_freq_index<pisa::simdbp_block, true>;

class InvalidEncoding: std::exception {
  public:
    explicit InvalidEncoding(std::string_view encoding)
        : m_message(fmt::format("Invalid encoding: {}", encoding))
    {}
    [[nodiscard]] auto what() const noexcept -> char const* override { return m_message.c_str(); }

  private:
    std::string m_message;
};

namespace detail {

    template <typename Index>
    struct IndexTypeMarker {
        using type = Index;
    };

    using IndexType = std::variant<
        IndexTypeMarker<ef_index>,
        IndexTypeMarker<single_index>,
        IndexTypeMarker<pefuniform_index>,
        IndexTypeMarker<pefopt_index>,
        IndexTypeMarker<block_optpfor_index>,
        IndexTypeMarker<block_varintg8iu_index>,
        IndexTypeMarker<block_streamvbyte_index>,
        IndexTypeMarker<block_maskedvbyte_index>,
        IndexTypeMarker<block_interpolative_index>,
        IndexTypeMarker<block_qmx_index>,
        IndexTypeMarker<block_varintgb_index>,
        IndexTypeMarker<block_simple8b_index>,
        IndexTypeMarker<block_simple16_index>,
        IndexTypeMarker<block_simdbp_index>,
        IndexTypeMarker<profiling_block_optpfor_index>,
        IndexTypeMarker<profiling_block_varintg8iu_index>,
        IndexTypeMarker<profiling_block_streamvbyte_index>,
        IndexTypeMarker<profiling_block_maskedvbyte_index>,
        IndexTypeMarker<profiling_block_interpolative_index>,
        IndexTypeMarker<profiling_block_qmx_index>,
        IndexTypeMarker<profiling_block_varintgb_index>,
        IndexTypeMarker<profiling_block_simple8b_index>,
        IndexTypeMarker<profiling_block_simple16_index>,
        IndexTypeMarker<profiling_block_simdbp_index>>;

    template <typename I>
    [[nodiscard]] constexpr auto index_name() noexcept -> std::string_view
    {
        if constexpr (std::is_same_v<I, ef_index>) {
            return "ef";
        } else if constexpr (std::is_same_v<I, single_index>) {
            return "single";
        } else if constexpr (std::is_same_v<I, pefuniform_index>) {
            return "pefuniform";
        } else if constexpr (std::is_same_v<I, pefopt_index>) {
            return "pefopt";
        } else if constexpr (std::is_same_v<I, block_optpfor_index>) {
            return "block_optpfor";
        } else if constexpr (std::is_same_v<I, block_varintg8iu_index>) {
            return "block_varintg8iu";
        } else if constexpr (std::is_same_v<I, block_streamvbyte_index>) {
            return "block_streamvbyte";
        } else if constexpr (std::is_same_v<I, block_maskedvbyte_index>) {
            return "block_maskedvbyte";
        } else if constexpr (std::is_same_v<I, block_interpolative_index>) {
            return "block_interpolative";
        } else if constexpr (std::is_same_v<I, block_qmx_index>) {
            return "block_qmx";
        } else if constexpr (std::is_same_v<I, block_varintgb_index>) {
            return "block_varintgb";
        } else if constexpr (std::is_same_v<I, block_simple8b_index>) {
            return "block_simple8b";
        } else if constexpr (std::is_same_v<I, block_simple16_index>) {
            return "block_simple16";
        } else if constexpr (std::is_same_v<I, block_simdbp_index>) {
            return "block_simdbp";
        } else if constexpr (std::is_same_v<I, profiling_block_optpfor_index>) {
            return "block_optpfor";
        } else if constexpr (std::is_same_v<I, profiling_block_varintg8iu_index>) {
            return "block_varintg8iu";
        } else if constexpr (std::is_same_v<I, profiling_block_streamvbyte_index>) {
            return "block_streamvbyte";
        } else if constexpr (std::is_same_v<I, profiling_block_maskedvbyte_index>) {
            return "block_maskedvbyte";
        } else if constexpr (std::is_same_v<I, profiling_block_interpolative_index>) {
            return "block_interpolative";
        } else if constexpr (std::is_same_v<I, profiling_block_qmx_index>) {
            return "block_qmx";
        } else if constexpr (std::is_same_v<I, profiling_block_varintgb_index>) {
            return "block_varintgb";
        } else if constexpr (std::is_same_v<I, profiling_block_simple8b_index>) {
            return "block_simple8b";
        } else if constexpr (std::is_same_v<I, profiling_block_simple16_index>) {
            return "block_simple16";
        } else if constexpr (std::is_same_v<I, profiling_block_simdbp_index>) {
            return "block_simdbp";
        }
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
    auto resolve_index_n(std::string_view encoding, Fn fn) -> bool
    {
        using T = typename std::variant_alternative_t<I, IndexType>::type;

        if (encoding == detail::index_name<T>()) {
            if constexpr (Profile) {
                using P = typename add_profiling<T>::type;
                fn(IndexType(IndexTypeMarker<P>{}));
            } else {
                fn(IndexType(IndexTypeMarker<T>{}));
            }
            return true;
        }
        return false;
    }

    template <bool Profile, std::size_t... I>
    auto resolve_index_type(std::string_view encoding, std::integer_sequence<std::size_t, I...>)
        -> IndexType
    {
        IndexType index_type{};
        bool loaded =
            (resolve_index_n<Profile, I>(encoding, [&](IndexType i) { index_type = i; }) || ...);
        if (!loaded) {
            throw InvalidEncoding(encoding);
        }
        return index_type;
    }

    template <bool Profile>
    [[nodiscard]] auto resolve_index_type(std::string_view encoding) -> IndexType
    {
        std::string full_index_type = fmt::format("{}_index", encoding);
        constexpr auto num_types = std::variant_size_v<IndexType>;
        return detail::resolve_index_type<Profile>(encoding, std::make_index_sequence<num_types>{});
    }

};  // namespace detail

using Encodings = std::array<std::string_view, variant_size_v<detail::IndexType>>;

template <std::size_t I>
constexpr void push_encoding(Encodings& encodings)
{
    encodings[I] =
        detail::index_name<typename std::variant_alternative_t<I, detail::IndexType>::type>();
}

template <std::size_t... I>
[[nodiscard]] constexpr auto encodings(std::integer_sequence<std::size_t, I...>) -> Encodings
{
    Encodings encodings;
    (push_encoding<I>(encodings), ...);
    return encodings;
}

[[nodiscard]] inline constexpr auto encodings() -> Encodings
{
    return encodings(std::make_index_sequence<variant_size_v<detail::IndexType>>{});
}

class IndexType {
    detail::IndexType m_type_marker;

    explicit IndexType(detail::IndexType type_marker) : m_type_marker(type_marker) {}

  public:
    static auto resolve(std::string_view encoding) -> IndexType
    {
        return IndexType(detail::resolve_index_type<false>(encoding));
    }

    static auto resolve_profiling(std::string_view encoding) -> IndexType
    {
        return IndexType(detail::resolve_index_type<true>(encoding));
    }

    template <typename Fn>
    void execute(Fn fn)
    {
        std::visit(fn, m_type_marker);
    }

    template <typename Fn>
    void load_and_execute(std::string const& index_path, Fn fn)
    {
        std::visit(
            [&](auto marker) {
                using I = typename decltype(marker)::type;
                fn(I(MemorySource::mapped_file(index_path)));
            },
            m_type_marker);
    }
};

}  // namespace pisa
