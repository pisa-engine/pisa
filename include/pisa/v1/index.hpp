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

namespace concepts {

    // template <typename T>
    // concept bool CursorLike = requires(T cursor, DocId docid, Position pos)
    //{
    //    { cursor.reset() } -> void;
    //    { cursor.next() } -> void;
    //    { cursor.next_geq(docid) } -> void;
    //    { cursor.move(pos) } -> void;
    //    { cursor.docid() } -> DocId;
    //    { cursor.position() } -> Position;
    //    { cursor.size() } -> std::size_t;
    //};

}

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

    [[nodiscard]] constexpr auto operator*() const -> T
    {
        return bit_cast<T>(gsl::as_bytes(m_bytes.subspan(m_current, sizeof(T))));
    }
    constexpr auto next() -> tl::optional<T>
    {
        step();
        return empty() ? tl::nullopt : tl::make_optional(operator*()());
    }
    constexpr void step() { m_current += sizeof(T); }
    [[nodiscard]] constexpr auto empty() const noexcept -> bool { return m_current == m_bytes.size(); }
    [[nodiscard]] constexpr auto position() const noexcept -> std::size_t { return m_current; }
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

template <typename KeyCursor, typename PayloadCursor>
struct ZipCursor {
    using Key = decltype(*std::declval<KeyCursor>());
    using Payload = decltype(*std::declval<PayloadCursor>());

    constexpr auto operator*() -> Key { return *m_key_cursor; }
    constexpr auto next() -> tl::optional<std::pair<Key, Payload>>
    {
        return m_key_cursor.next().and_then([&](Key key) {
            return m_payload_cursor.next().map(
                [key](Payload payload) { return std::make_pair(key, payload); });
        });
    }
    constexpr void step()
    {
        m_key_cursor.step();
        m_payload_cursor.step();
    }
    constexpr auto empty() -> bool { return m_key_cursor.empty(); }
    [[nodiscard]] constexpr auto size() const -> std::size_t { return m_key_cursor.size(); }

   private:
    KeyCursor m_key_cursor;
    PayloadCursor m_payload_cursor;
};

template <typename DocumentReader, typename PayloadReader>
struct ZippedIndex {
    using DocumentCursor =
        decltype(std::declval<DocumentReader>().read(std::declval<gsl::span<std::byte>>()));
    using PayloadCursor =
        decltype(std::declval<PayloadReader>().read(std::declval<gsl::span<std::byte>>()));
    static_assert(std::is_same_v<decltype(*std::declval<DocumentCursor>()), DocId>);
    // static_assert(std::is_same_v<decltype(*std::declval<PayloadCursor>()), Frequency>);

    [[nodiscard]] auto cursor(TermId term) -> ZipCursor<DocumentCursor, PayloadCursor>;

   private:
    [[nodiscard]] auto fetch(TermId term) -> gsl::span<std::byte>;

    DocumentReader m_document_reader;
    PayloadReader m_payload_reader;
};

} // namespace pisa::v1
