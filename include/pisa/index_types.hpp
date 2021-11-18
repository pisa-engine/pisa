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

/// Exception thrown when an invalid index encoding is requested. See `IndexType::resolve`.
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

    /// This object holds an index type; this can be passed to a visitor without constructing the
    /// index itself.
    template <typename Index>
    struct IndexTypeMarker {
        using type = Index;
    };

    /// Variant type listing all supported index types.
    ///
    /// NOTE: To support a new index type in the tools, it must be added here.
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

}  // namespace detail

/// Number of supported index types.
constexpr std::size_t NUM_INDEX_TYPES = variant_size_v<detail::IndexType>;

namespace detail {

    /// Returns the string representation of the index type `I`.
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

    /// Attempts to match the given encoding name to the index type at position `N` in the index
    /// type variant.
    ///
    /// The function passed in the argument assigns the resolved type object if it is matched with
    /// the encoding. If the match succeeds, this function returns `true`, otherwise, it returns
    /// `false`.
    template <bool Profile, std::size_t N, typename Fn>
    auto resolve_index_n(std::string_view encoding, Fn&& fn) -> bool
    {
        using T = typename std::variant_alternative_t<N, IndexType>::type;

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

    /// Returns the type object of the index with the given encoding, or throws `InvalidEncoding` if
    /// the given encoding name does not match any index type.
    template <bool Profile, std::size_t... N>
    auto resolve_index_type(std::string_view encoding, std::integer_sequence<std::size_t, N...>)
        -> IndexType
    {
        IndexType index_type{};
        auto update = [&](IndexType i) { index_type = i; };
        bool loaded = (resolve_index_n<Profile, N>(encoding, update) || ...);
        if (!loaded) {
            throw InvalidEncoding(encoding);
        }
        return index_type;
    }

    /// Returns the type object of the index with the given encoding, or throws `InvalidEncoding` if
    /// the given encoding name does not match any index type.
    template <bool Profile>
    [[nodiscard]] auto resolve_index_type(std::string_view encoding) -> IndexType
    {
        std::string full_index_type = fmt::format("{}_index", encoding);
        constexpr auto num_types = std::variant_size_v<IndexType>;
        return detail::resolve_index_type<Profile>(encoding, std::make_index_sequence<num_types>{});
    }

    template <std::size_t I>
    constexpr void push_encoding(std::array<std::string_view, NUM_INDEX_TYPES>& encodings)
    {
        encodings[I] =
            detail::index_name<typename std::variant_alternative_t<I, detail::IndexType>::type>();
    }

    template <std::size_t... I>
    [[nodiscard]] constexpr auto encodings(std::integer_sequence<std::size_t, I...>)
        -> std::array<std::string_view, NUM_INDEX_TYPES>
    {
        std::array<std::string_view, NUM_INDEX_TYPES> encodings;
        (push_encoding<I>(encodings), ...);
        return encodings;
    }

};  // namespace detail

/// Returns an array of the names of all supported index types.
[[nodiscard]] inline constexpr auto encodings() -> std::array<std::string_view, NUM_INDEX_TYPES>
{
    return detail::encodings(std::make_index_sequence<variant_size_v<detail::IndexType>>{});
}

/// Represents an index type.
///
/// This type's objects are used execute monomorphized code on an index of a particular type.
/// See `execute()` and `load_and_execute()` for more details.
class IndexType {
    detail::IndexType m_type_marker;

    explicit IndexType(detail::IndexType type_marker) : m_type_marker(type_marker) {}

  public:
    /// Resolves the index type beased on its string representation.
    static auto resolve(std::string_view encoding) -> IndexType
    {
        return IndexType(detail::resolve_index_type<false>(encoding));
    }

    /// Same as `resolve` but use a profiling index.
    static auto resolve_profiling(std::string_view encoding) -> IndexType
    {
        return IndexType(detail::resolve_index_type<true>(encoding));
    }

    /// Executes a templated `fn`, passing to it an index type marker that can be used to access the
    /// index type.
    ///
    /// # Example
    ///
    /// ```
    /// IndexType::resolve("block_simdbp").execute([](auto marker) {
    ///     using index_type = typename decltype(marker)::type;
    /// })
    /// ```
    template <typename Fn>
    void execute(Fn fn)
    {
        std::visit(fn, m_type_marker);
    }

    /// Loads an index from the given path, and executes a templated `fn`, passing to it the loaded
    /// index.
    ///
    /// # Example
    ///
    /// ```
    /// IndexType::resolve("block_simdbp").load_and_execute(index_path, [](auto&& index) {
    ///     // use `index`, which ehere is of type `block_simdbp_index`
    /// })
    /// ```
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
