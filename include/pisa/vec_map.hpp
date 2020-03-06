#pragma once

#include <fstream>
#include <string>
#include <vector>

namespace pisa {

template <typename Index, typename Iterator>
struct EnumerateIterator {
  private:
    using Value = typename std::iterator_traits<Iterator>::value_type;

  public:
    using difference_type = std::ptrdiff_t;
    using value_type = std::pair<Index, typename std::iterator_traits<Iterator>::value_type>;
    using pointer = std::pair<Index*, typename std::iterator_traits<Iterator>::pointer>;
    using reference = std::pair<Index const&, typename std::iterator_traits<Iterator>::reference>;
    using iterator_category = std::forward_iterator_tag;

    constexpr EnumerateIterator(Iterator iter, Index init)
        : m_current_index(std::move(init)), m_value_iterator(std::move(iter))
    {}
    ~EnumerateIterator() = default;
    constexpr EnumerateIterator(EnumerateIterator const&) = default;
    constexpr EnumerateIterator(EnumerateIterator&&) noexcept = default;
    constexpr auto operator=(EnumerateIterator const&) -> EnumerateIterator& = default;
    constexpr auto operator=(EnumerateIterator&&) noexcept -> EnumerateIterator& = default;
    constexpr auto operator++() -> EnumerateIterator&
    {
        ++m_value_iterator;
        ++m_current_index;
        return *this;
    }
    constexpr auto operator++(int) -> EnumerateIterator
    {
        EnumerateIterator retval = *this;
        ++(*this);
        return retval;
    }
    [[nodiscard]] constexpr auto operator==(EnumerateIterator other) const -> bool
    {
        return m_value_iterator == other.m_value_iterator;
    }
    [[nodiscard]] constexpr auto operator!=(EnumerateIterator other) const -> bool
    {
        return !(m_value_iterator == other.m_value_iterator);
    }
    [[nodiscard]] constexpr auto operator*() const -> reference
    {
        return reference(m_current_index, *m_value_iterator);
    }

  private:
    Index m_current_index;
    Iterator m_value_iterator;
};

template <typename Index, typename Iterator>
struct Enumerate {
    template <typename Container>
    constexpr explicit Enumerate(Container&& container, Index init = Index{})
        : m_init(std::move(init)), m_value_begin(container.begin()), m_value_end(container.end())
    {}

    [[nodiscard]] constexpr auto begin() -> EnumerateIterator<Index, Iterator>
    {
        return EnumerateIterator<Index, Iterator>(m_value_begin, m_init);
    }

    [[nodiscard]] constexpr auto end() -> EnumerateIterator<Index, Iterator>
    {
        return EnumerateIterator<Index, Iterator>(m_value_end, m_init);
    }

    [[nodiscard]] constexpr auto begin() const -> EnumerateIterator<Index, Iterator>
    {
        return EnumerateIterator<Index, Iterator>(m_value_begin, m_init);
    }

    [[nodiscard]] constexpr auto end() const -> EnumerateIterator<Index, Iterator>
    {
        return EnumerateIterator<Index, Iterator>(m_value_end, m_init);
    }

    [[nodiscard]] constexpr auto cbegin() const -> EnumerateIterator<Index, Iterator>
    {
        return begin();
    }

    [[nodiscard]] constexpr auto cend() const -> EnumerateIterator<Index, Iterator>
    {
        return end();
    }

    [[nodiscard]] constexpr auto size() const -> std::size_t
    {
        return std::distance(m_value_begin, m_value_end);
    }

    [[nodiscard]] constexpr auto collect() const
        -> std::vector<std::pair<Index, typename std::iterator_traits<Iterator>::value_type>>
    {
        std::vector<std::pair<Index, typename std::iterator_traits<Iterator>::value_type>> vec(size());
        std::copy(begin(), end(), vec.begin());
        return vec;
    }

  private:
    Index m_init;
    Iterator m_value_begin;
    Iterator m_value_end;
};

}  // namespace pisa

namespace std {

template <typename Index, typename Iterator>
struct iterator_traits<::pisa::EnumerateIterator<Index, Iterator>> {
    using difference_type = typename ::pisa::EnumerateIterator<Index, Iterator>::difference_type;
    using value_type = typename ::pisa::EnumerateIterator<Index, Iterator>::value_type;
    using pointer = typename ::pisa::EnumerateIterator<Index, Iterator>::pointer;
    using reference = typename ::pisa::EnumerateIterator<Index, Iterator>::reference;
    using iterator_category = typename ::pisa::EnumerateIterator<Index, Iterator>::iterator_category;
};

}  // namespace std

namespace pisa {

/// An associative map from type `K` that can be cast to `size_type`.
/// It maps all values between 0 and `size()`.
///
/// It derives from `std::vector<V>` and works essentially like one.
/// The difference is that you define a key type as well, the outcome
/// being that if you use a strong type key, you can differentiate between
/// `VecMap<IndexType_1, V>` and `VecMap<IndexType_2, V>`.
template <typename K, typename V = K, typename Allocator = std::allocator<V>>
class VecMap: protected std::vector<V> {
  public:
    using typename std::vector<V, Allocator>::value_type;
    using typename std::vector<V, Allocator>::reference;
    using typename std::vector<V, Allocator>::const_reference;
    using typename std::vector<V, Allocator>::size_type;
    using typename std::vector<V, Allocator>::iterator;
    using typename std::vector<V, Allocator>::const_iterator;

    using std::vector<V, Allocator>::assign;
    using std::vector<V, Allocator>::get_allocator;

    using std::vector<V, Allocator>::at;
    using std::vector<V, Allocator>::front;
    using std::vector<V, Allocator>::back;
    using std::vector<V, Allocator>::data;

    using std::vector<V, Allocator>::begin;
    using std::vector<V, Allocator>::cbegin;
    using std::vector<V, Allocator>::end;
    using std::vector<V, Allocator>::cend;
    using std::vector<V, Allocator>::rbegin;
    using std::vector<V, Allocator>::crbegin;
    using std::vector<V, Allocator>::rend;
    using std::vector<V, Allocator>::crend;

    using std::vector<V, Allocator>::empty;
    using std::vector<V, Allocator>::size;
    using std::vector<V, Allocator>::max_size;
    using std::vector<V, Allocator>::reserve;
    using std::vector<V, Allocator>::capacity;
    using std::vector<V, Allocator>::shrink_to_fit;

    using std::vector<V, Allocator>::clear;
    using std::vector<V, Allocator>::insert;
    using std::vector<V, Allocator>::emplace;
    using std::vector<V, Allocator>::erase;
    using std::vector<V, Allocator>::push_back;
    using std::vector<V, Allocator>::emplace_back;
    using std::vector<V, Allocator>::pop_back;
    using std::vector<V, Allocator>::resize;
    using std::vector<V, Allocator>::swap;

    VecMap() noexcept(noexcept(Allocator())) : std::vector<V, Allocator>() {}
    explicit VecMap(Allocator const& alloc) noexcept : std::vector<V, Allocator>(alloc) {}
    VecMap(size_type count, V const& value, Allocator const& alloc = Allocator())
        : std::vector<V, Allocator>(count, value, alloc)
    {}
    explicit VecMap(size_type count, Allocator const& alloc = Allocator())
        : std::vector<V, Allocator>(count, alloc)
    {}
    template <class InputIt>
    VecMap(InputIt first, InputIt last, Allocator const& alloc = Allocator())
        : std::vector<V, Allocator>(first, last, alloc)
    {}
    VecMap(VecMap const& other) : std::vector<V, Allocator>(other) {}
    VecMap(VecMap const& other, const Allocator& alloc) : std::vector<V, Allocator>(other, alloc) {}
    VecMap(VecMap&& other) noexcept : std::vector<V, Allocator>(other) {}
    VecMap(VecMap&& other, Allocator const& alloc) : std::vector<V, Allocator>(other, alloc) {}
    VecMap(std::initializer_list<V> init, Allocator const& alloc = Allocator())
        : std::vector<V, Allocator>(init, alloc)
    {}
    ~VecMap() = default;

    auto operator=(VecMap const& other) -> VecMap&
    {
        if (this != &other) {
            std::vector<V, Allocator>::operator=(other);
        }
        return *this;
    };
    auto operator=(VecMap&& other) noexcept -> VecMap&
    {
        std::vector<V, Allocator>::operator=(other);
        return *this;
    };
    auto operator=(std::initializer_list<V> init) -> VecMap&
    {
        std::vector<V, Allocator>::operator=(init);
        return *this;
    }

    auto operator[](K key) -> reference
    {
        return std::vector<V, Allocator>::operator[](static_cast<size_type>(key));
    }
    auto operator[](K key) const -> const_reference
    {
        return std::vector<V, Allocator>::operator[](static_cast<size_type>(key));
    }
    auto at(K key) -> reference
    {
        return std::vector<V, Allocator>::at(static_cast<size_type>(key));
    }
    auto at(K key) const -> const_reference
    {
        return std::vector<V, Allocator>::at(static_cast<size_type>(key));
    }

    auto as_vector() const -> std::vector<V> const& { return *this; }
    auto as_vector() -> std::vector<V>& { return *this; }

    [[nodiscard]] auto entries() const -> Enumerate<K, typename std::vector<V>::const_iterator>
    {
        return Enumerate<K, typename std::vector<V>::const_iterator>(*this, static_cast<K>(0U));
    }
};

template <class K, class V, class Alloc>
void swap(VecMap<K, V, Alloc>& lhs, VecMap<K, V, Alloc>& rhs) noexcept(noexcept(lhs.swap(rhs)))
{
    return lhs.swap(rhs);
}

template <class K, class V, class Alloc>
auto operator==(const VecMap<K, V, Alloc>& lhs, const VecMap<K, V, Alloc>& rhs) -> bool
{
    return lhs.as_vector() == rhs.as_vector();
}
template <class K, class V, class Alloc>
auto operator!=(const VecMap<K, V, Alloc>& lhs, const VecMap<K, V, Alloc>& rhs) -> bool
{
    return lhs.as_vector() != rhs.as_vector();
}
template <class K, class V, class Alloc>
auto operator<(const VecMap<K, V, Alloc>& lhs, const VecMap<K, V, Alloc>& rhs) -> bool
{
    return lhs.as_vector() < rhs.as_vector();
}
template <class K, class V, class Alloc>
auto operator<=(const VecMap<K, V, Alloc>& lhs, const VecMap<K, V, Alloc>& rhs) -> bool
{
    return lhs.as_vector() <= rhs.as_vector();
}
template <class K, class V, class Alloc>
auto operator>(const VecMap<K, V, Alloc>& lhs, const VecMap<K, V, Alloc>& rhs) -> bool
{
    return lhs.as_vector() > rhs.as_vector();
}
template <class K, class V, class Alloc>
auto operator>=(const VecMap<K, V, Alloc>& lhs, const VecMap<K, V, Alloc>& rhs) -> bool
{
    return lhs.as_vector() >= rhs.as_vector();
}

template <typename Key>
[[nodiscard]] inline auto read_string_vec_map(std::string const& filename)
    -> VecMap<Key, std::string>
{
    VecMap<Key, std::string> vec;
    std::ifstream is(filename);
    std::string line;
    while (std::getline(is, line)) {
        vec.push_back(std::move(line));
    }
    return vec;
}

}  // namespace pisa
