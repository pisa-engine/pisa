#pragma once

#include <cstdint>
#include <cstring>
#include <type_traits>
#include <utility>

#include <gsl/gsl_assert>
#include <gsl/span>
#include <tl/optional.hpp>

namespace pisa::v1 {

template <class T>
constexpr auto bit_cast(gsl::span<const std::byte> mem) -> T
{
    T dst;
    std::memcpy(&dst, mem.data(), sizeof(T));
    return dst;
}

using TermId = std::uint32_t;
using DocId = std::uint32_t;
using Frequency = std::uint32_t;

template <std::size_t I, typename Cursor>
auto payload(Cursor &cursor) -> typename std::tuple_element<I, typename Cursor::Payload>::type &;

template <std::size_t I, typename Cursor>
auto payload(Cursor const &cursor) ->
    typename std::tuple_element<I, typename Cursor::Payload>::type const &;

template <typename Cursor>
auto payload(Cursor &cursor) -> typename std::tuple_element<0, typename Cursor::Payload>::type &
{
    return payload<0>(cursor);
}

template <typename Cursor>
auto payload(Cursor const &cursor) ->
    typename std::tuple_element<0, typename Cursor::Payload>::type const &
{
    return payload<0>(cursor);
}

template <typename T>
struct RawCursor {
    static_assert(std::is_trivial<T>::value);

    explicit constexpr RawCursor(gsl::span<const std::byte> bytes) : m_bytes(bytes)
    {
        Expects(bytes.size() % sizeof(T) == 0);
    }

    constexpr auto operator*() -> T
    {
        return bit_cast<T>(gsl::as_bytes(m_bytes.subspan(m_current, sizeof(T))));
    }
    constexpr auto next() -> tl::optional<T>
    {
        step();
        return empty() ? tl::nullopt : tl::make_optional(operator*()());
    }
    constexpr void step() { m_current += sizeof(T); }
    constexpr auto empty() -> bool { return m_current == m_bytes.size(); }
    [[nodiscard]] constexpr auto size() const -> std::size_t { return m_bytes.size() / sizeof(T); }

   private:
    std::size_t m_current = 0;
    gsl::span<const std::byte> m_bytes;
};

template <typename T>
struct RawReader {
    static_assert(std::is_trivial<T>::value);

    [[nodiscard]] auto read(gsl::span<const std::byte> bytes) const -> RawCursor<T>
    {
        return RawCursor<T>(bytes.subspan(sizeof(std::uint64_t)));
    }
};

struct IndexFactory {
};

template <typename Reader>
struct Index {
    using Cursor = decltype(std::declval<Reader>().read(std::declval<gsl::span<std::byte>>()));
    static_assert(std::is_same_v<decltype(*std::declval<Cursor>()), DocId>);
    static_assert(std::is_same_v<decltype(::pisa::v1::payload(std::declval<Cursor>())), Frequency>);

    [[nodiscard]] auto cursor(TermId term) -> Cursor { return m_reader.read(fetch(term)); }

   private:
    [[nodiscard]] auto fetch(TermId term) -> gsl::span<std::byte>;

    Reader m_reader;
};

template <typename DocumentReader, typename FrequencyReader>
struct ZipCursor;

template <typename DocumentReader, typename FrequencyReader>
struct Index2 {
    using DocumentCursor =
        decltype(std::declval<DocumentReader>().read(std::declval<gsl::span<std::byte>>()));
    using FrequencyCursor =
        decltype(std::declval<FrequencyReader>().read(std::declval<gsl::span<std::byte>>()));
    static_assert(std::is_same_v<decltype(*std::declval<DocumentCursor>()), DocId>);
    static_assert(std::is_same_v<decltype(*std::declval<FrequencyCursor>()), Frequency>);

    [[nodiscard]] auto cursor(TermId term) -> ZipCursor<DocumentCursor, FrequencyCursor>;

   private:
    [[nodiscard]] auto fetch(TermId term) -> gsl::span<std::byte>;

    DocumentReader m_document_reader;
    FrequencyReader m_frequency_reader;
};

} // namespace pisa::v1
