#pragma once

#include <vector>

#include <range/v3/view/iota.hpp>
#include <range/v3/view/zip.hpp>

namespace pisa {

/// An associative map from type `K` that can be cast to `size_type`.
/// It maps all values between 0 and `size()`.
///
/// It derives from `std::vector<V>` and works essentially like one.
/// The difference is that you define a key type as well, the outcome
/// being that if you use a strong type key, you can differentiate between
/// `VecMap<IndexType_1, V>` and `VecMap<IndexType_2, V>`.
template <typename K, typename V = K, typename Allocator = std::allocator<V>>
class VecMap : protected std::vector<V> {
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
    explicit VecMap(Allocator const &alloc) noexcept : std::vector<V, Allocator>(alloc) {}
    VecMap(size_type count, V const &value, Allocator const &alloc = Allocator())
        : std::vector<V, Allocator>(count, value, alloc)
    {}
    explicit VecMap(size_type count, Allocator const &alloc = Allocator())
        : std::vector<V, Allocator>(count, alloc)
    {}
    template <class InputIt>
    VecMap(InputIt first, InputIt last, Allocator const &alloc = Allocator())
        : std::vector<V, Allocator>(first, last, alloc)
    {}
    VecMap(VecMap const &other) : std::vector<V, Allocator>(other) {}
    VecMap(VecMap const &other, const Allocator &alloc) : std::vector<V, Allocator>(other, alloc) {}
    VecMap(VecMap &&other) noexcept : std::vector<V, Allocator>(other) {}
    VecMap(VecMap &&other, Allocator const &alloc) : std::vector<V, Allocator>(other, alloc) {}
    VecMap(std::initializer_list<V> init, Allocator const &alloc = Allocator())
        : std::vector<V, Allocator>(init, alloc)
    {}
    ~VecMap() = default;

    VecMap &operator=(VecMap const &other)
    {
        std::vector<V, Allocator>::operator=(other);
        return *this;
    };
    VecMap &operator=(VecMap &&other) noexcept
    {
        std::vector<V, Allocator>::operator=(other);
        return *this;
    };
    VecMap &operator=(std::initializer_list<V> init)
    {
        std::vector<V, Allocator>::operator=(init);
        return *this;
    }

    reference operator[](K key)
    {
        return std::vector<V, Allocator>::operator[](static_cast<size_type>(key));
    }
    const_reference operator[](K key) const
    {
        return std::vector<V, Allocator>::operator[](static_cast<size_type>(key));
    }
    reference at(K key) { return std::vector<V, Allocator>::at(static_cast<size_type>(key)); }
    const_reference at(K key) const
    {
        return std::vector<V, Allocator>::at(static_cast<size_type>(key));
    }

    std::vector<V> const &as_vector() const { return *this; }

    [[nodiscard]] auto entries() const
    {
        return ranges::view::zip(ranges::view::iota(static_cast<K>(0u), static_cast<K>(size())),
                                 as_vector());
    }
};

template <class K, class V, class Alloc>
void swap(VecMap<K, V, Alloc> &lhs, VecMap<K, V, Alloc> &rhs) noexcept(noexcept(lhs.swap(rhs)))
{
    return lhs.swap(rhs);
}

template <class K, class V, class Alloc>
bool operator==(const VecMap<K, V, Alloc> &lhs, const VecMap<K, V, Alloc> &rhs)
{
    return lhs.as_vector() == rhs.as_vector();
}
template <class K, class V, class Alloc>
bool operator!=(const VecMap<K, V, Alloc> &lhs, const VecMap<K, V, Alloc> &rhs)
{
    return lhs.as_vector() != rhs.as_vector();
}
template <class K, class V, class Alloc>
bool operator<(const VecMap<K, V, Alloc> &lhs, const VecMap<K, V, Alloc> &rhs)
{
    return lhs.as_vector() < rhs.as_vector();
}
template <class K, class V, class Alloc>
bool operator<=(const VecMap<K, V, Alloc> &lhs, const VecMap<K, V, Alloc> &rhs)
{
    return lhs.as_vector() <= rhs.as_vector();
}
template <class K, class V, class Alloc>
bool operator>(const VecMap<K, V, Alloc> &lhs, const VecMap<K, V, Alloc> &rhs)
{
    return lhs.as_vector() > rhs.as_vector();
}
template <class K, class V, class Alloc>
bool operator>=(const VecMap<K, V, Alloc> &lhs, const VecMap<K, V, Alloc> &rhs)
{
    return lhs.as_vector() >= rhs.as_vector();
}

} // namespace pisa
