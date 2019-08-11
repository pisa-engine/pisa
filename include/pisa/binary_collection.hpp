#pragma once

#include <cassert>
#include <cstdint>
#include <iterator>
#include <string>

#include <mio/mmap.hpp>

namespace pisa {

namespace detail {
    template <typename Sequence, typename Iterator>
    void read_next_sequence(Iterator &iter);
}

struct BinaryCollection {
   public:
    using size_type = std::size_t;
    using posting_type = uint32_t;
    using pointer = posting_type *;
    using const_pointer = posting_type const *;

    BinaryCollection(char const *filename);
    BinaryCollection(std::string const &filename);

    struct sequence {
       public:
        sequence() = default;
        sequence(pointer begin, pointer end) noexcept;

        [[nodiscard]] auto begin() const noexcept -> pointer;
        [[nodiscard]] auto end() const noexcept -> pointer;
        [[nodiscard]] auto cbegin() const noexcept -> const_pointer;
        [[nodiscard]] auto cend() const noexcept -> const_pointer;
        [[nodiscard]] auto size() const noexcept -> size_type;
        [[nodiscard]] auto back() const -> posting_type;

       private:
        pointer m_begin;
        pointer m_end;
    };

    struct const_sequence {
       public:
        const_sequence() = default;
        const_sequence(const_pointer begin, const_pointer end) noexcept;

        [[nodiscard]] auto begin() const noexcept -> const_pointer;
        [[nodiscard]] auto end() const noexcept -> const_pointer;
        [[nodiscard]] auto cbegin() const noexcept -> const_pointer;
        [[nodiscard]] auto cend() const noexcept -> const_pointer;
        [[nodiscard]] auto size() const noexcept -> size_type;
        [[nodiscard]] auto back() const -> posting_type;

       private:
        const_pointer m_begin;
        const_pointer m_end;
    };

    struct iterator {
        using difference_type = std::ptrdiff_t;

        iterator() = default;
        [[nodiscard]] auto operator*() const -> sequence const &;
        [[nodiscard]] auto operator*() -> sequence &;
        [[nodiscard]] auto operator-> () const -> sequence const *;
        [[nodiscard]] auto operator-> () -> sequence *;
        iterator &operator++();
        [[nodiscard]] auto operator==(iterator const &other) const -> bool;
        [[nodiscard]] auto operator!=(iterator const &other) const -> bool;

       private:
        friend struct BinaryCollection;
        friend void detail::read_next_sequence<sequence, iterator>(iterator &a);

        iterator(BinaryCollection const *coll, size_type pos);

        pointer m_data = nullptr;
        size_type m_data_size = 0;
        size_type m_pos = 0;
        size_type m_next_pos = 0;
        sequence m_current_seqence;
    };

    struct const_iterator {
        using difference_type = std::ptrdiff_t;

        const_iterator() = default;
        [[nodiscard]] auto operator*() const -> const_sequence const &;
        [[nodiscard]] auto operator-> () const -> const_sequence const *;
        const_iterator &operator++();
        [[nodiscard]] auto operator==(const_iterator const &other) const -> bool;
        [[nodiscard]] auto operator!=(const_iterator const &other) const -> bool;

       private:
        friend struct BinaryCollection;
        friend void detail::read_next_sequence<const_sequence, const_iterator>(const_iterator &a);

        const_iterator(BinaryCollection const *coll, size_type pos);

        const_pointer m_data = nullptr;
        size_type m_data_size = 0;
        size_type m_pos = 0;
        size_type m_next_pos = 0;
        const_sequence m_current_seqence;
    };

    [[nodiscard]] auto begin() -> iterator;
    [[nodiscard]] auto end() -> iterator;
    [[nodiscard]] auto begin() const -> const_iterator;
    [[nodiscard]] auto end() const -> const_iterator;
    [[nodiscard]] auto cbegin() const -> const_iterator;
    [[nodiscard]] auto cend() const -> const_iterator;

   private:
    mio::mmap_sink m_file;
    pointer m_data;
    size_t m_data_size;
};

[[nodiscard]] inline BinaryCollection::sequence const &BinaryCollection::iterator::operator*() const
{
    return m_current_seqence;
}

[[nodiscard]] inline BinaryCollection::sequence &BinaryCollection::iterator::operator*()
{
    return m_current_seqence;
}

[[nodiscard]] inline BinaryCollection::sequence const *BinaryCollection::iterator::operator->()
    const
{
    return &m_current_seqence;
}

[[nodiscard]] inline BinaryCollection::sequence *BinaryCollection::iterator::operator->()
{
    return &m_current_seqence;
}

[[nodiscard]] inline bool BinaryCollection::iterator::operator==(iterator const &other) const
{
    assert(m_data == other.m_data);
    assert(m_data_size == other.m_data_size);
    return m_pos == other.m_pos;
}

[[nodiscard]] inline bool BinaryCollection::iterator::operator!=(iterator const &other) const
{
    return !(*this == other);
}

[[nodiscard]] inline BinaryCollection::const_sequence const
    &BinaryCollection::const_iterator::operator*() const
{
    return m_current_seqence;
}

[[nodiscard]] inline BinaryCollection::const_sequence const
    *BinaryCollection::const_iterator::operator->() const
{
    return &m_current_seqence;
}

[[nodiscard]] inline bool BinaryCollection::const_iterator::operator==(
    const_iterator const &other) const
{
    assert(m_data == other.m_data);
    assert(m_data_size == other.m_data_size);
    return m_pos == other.m_pos;
}

[[nodiscard]] inline bool BinaryCollection::const_iterator::operator!=(
    const_iterator const &other) const
{
    return !(*this == other);
}

inline BinaryCollection::sequence::sequence(pointer begin, pointer end) noexcept
    : m_begin(begin), m_end(end)
{
}
[[nodiscard]] inline auto BinaryCollection::sequence::begin() const noexcept -> pointer
{
    return m_begin;
}
[[nodiscard]] inline auto BinaryCollection::sequence::end() const noexcept -> pointer
{
    return m_end;
}
[[nodiscard]] inline auto BinaryCollection::sequence::cbegin() const noexcept -> const_pointer
{
    return m_begin;
}
[[nodiscard]] inline auto BinaryCollection::sequence::cend() const noexcept -> const_pointer
{
    return m_end;
}
[[nodiscard]] inline auto BinaryCollection::sequence::size() const noexcept -> size_type
{
    return std::distance(m_begin, m_end);
}
[[nodiscard]] inline auto BinaryCollection::sequence::back() const -> posting_type
{
    assert(size());
    return *std::prev(m_end);
}

inline BinaryCollection::const_sequence::const_sequence(const_pointer begin,
                                                        const_pointer end) noexcept
    : m_begin(begin), m_end(end)
{
}
[[nodiscard]] inline auto BinaryCollection::const_sequence::begin() const noexcept -> const_pointer
{
    return m_begin;
}
[[nodiscard]] inline auto BinaryCollection::const_sequence::end() const noexcept -> const_pointer
{
    return m_end;
}
[[nodiscard]] inline auto BinaryCollection::const_sequence::cbegin() const noexcept -> const_pointer
{
    return m_begin;
}
[[nodiscard]] inline auto BinaryCollection::const_sequence::cend() const noexcept -> const_pointer
{
    return m_end;
}
[[nodiscard]] inline auto BinaryCollection::const_sequence::size() const noexcept -> size_type
{
    return std::distance(m_begin, m_end);
}
[[nodiscard]] inline auto BinaryCollection::const_sequence::back() const -> posting_type
{
    assert(size());
    return *std::prev(m_end);
}

} // namespace pisa

namespace std {

template <>
struct iterator_traits<::pisa::BinaryCollection::iterator> {
    using difference_type = std::ptrdiff_t;
    using value_type = ::pisa::BinaryCollection::sequence;
    using pointer = value_type *;
    using reference = value_type &;
    using iterator_category = std::input_iterator_tag;
};

template <>
struct iterator_traits<::pisa::BinaryCollection::const_iterator> {
    using difference_type = std::ptrdiff_t;
    using value_type = ::pisa::BinaryCollection::const_sequence;
    using pointer = value_type *;
    using reference = value_type &;
    using iterator_category = std::input_iterator_tag;
};

} // namespace std
