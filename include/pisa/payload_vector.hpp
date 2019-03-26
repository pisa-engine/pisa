#pragma once

#include <fstream>
#include <iostream>
#include <iterator>
#include <string_view>
#include <vector>

#include <gsl/span>
#include <gsl/gsl_assert>

namespace pisa {

namespace detail {

    using size_type = std::size_t;

    template <typename Payload_View = std::string_view>
    struct Payload_Vector_Iterator {
        using size_type = pisa::detail::size_type;
        using value_type = Payload_View;
        using difference_type = std::make_signed_t<size_type>;

        typename gsl::span<size_type const>::iterator offset_iter;
        typename gsl::span<std::byte const>::iterator payload_iter;

        constexpr auto operator++() -> Payload_Vector_Iterator &
        {
            ++offset_iter;
            std::advance(payload_iter, *offset_iter - *std::prev(offset_iter));
            return *this;
        }

        [[nodiscard]]
        constexpr auto operator++(int) -> Payload_Vector_Iterator
        {
            Payload_Vector_Iterator next_iter{offset_iter, payload_iter};
            ++(*this);
            return next_iter;
        }

        constexpr auto operator--() -> Payload_Vector_Iterator & { return *this -= 1; }

        [[nodiscard]]
        constexpr auto operator--(int) -> Payload_Vector_Iterator
        {
            Payload_Vector_Iterator next_iter{offset_iter, payload_iter};
            --(*this);
            return next_iter;
        }

        [[nodiscard]]
        constexpr auto operator+(size_type n) const -> Payload_Vector_Iterator
        {
            return {std::next(offset_iter, n),
                    std::next(payload_iter, *std::next(offset_iter, n) - *offset_iter)};
        }

        [[nodiscard]]
        constexpr auto operator+=(size_type n) -> Payload_Vector_Iterator &
        {
            std::advance(payload_iter, *std::next(offset_iter, n) - *offset_iter);
            std::advance(offset_iter, n);
            return *this;
        }

        [[nodiscard]]
        constexpr auto operator-(size_type n) const -> Payload_Vector_Iterator
        {
            return {std::prev(offset_iter, n),
                    std::prev(payload_iter, *offset_iter - *std::prev(offset_iter, n))};
        }

        [[nodiscard]]
        constexpr auto operator-=(size_type n) -> Payload_Vector_Iterator &
        {
            return *this = *this - n;
        }

        [[nodiscard]]
        constexpr auto operator-(Payload_Vector_Iterator const& other) -> difference_type
        {
            return offset_iter - other.offset_iter;
        }

        [[nodiscard]]
        constexpr auto operator*() -> value_type
        {
            return value_type(reinterpret_cast<char const *>(&*payload_iter),
                              *std::next(offset_iter) - *offset_iter);
        }

        [[nodiscard]] constexpr auto operator==(Payload_Vector_Iterator const &other) const -> bool
        {
            return offset_iter == other.offset_iter;
        }

        [[nodiscard]] constexpr auto operator!=(Payload_Vector_Iterator const &other) const -> bool
        {
            return offset_iter != other.offset_iter;
        }
    };

} // namespace detail

struct Payload_Vector_Container {
    using size_type = detail::size_type;

    std::vector<size_type> const offsets;
    std::vector<std::byte> const payloads;

    void to_file(std::string const &filename) const
    {
        std::ofstream is(filename);
        to_stream(is);
    }

    void to_stream(std::ostream &is) const
    {
        size_type length = offsets.size() - 1u;
        is.write(reinterpret_cast<char const *>(&length), sizeof(length));
        is.write(reinterpret_cast<char const *>(offsets.data()),
                 offsets.size() * sizeof(offsets[0]));
        is.write(reinterpret_cast<char const *>(payloads.data()), payloads.size());
    }

    template <typename InputIterator, typename PayloadEncodingFn>
    [[nodiscard]] static auto make(InputIterator first,
                                   InputIterator last,
                                   PayloadEncodingFn encoding_fn) -> Payload_Vector_Container
    {
        std::vector<size_type> offsets;
        offsets.push_back(0u);
        std::vector<std::byte> payloads;
        for (; first != last; ++first) {
            encoding_fn(*first, std::back_inserter(payloads));
            offsets.push_back(payloads.size());
        }
        return Payload_Vector_Container{std::move(offsets), std::move(payloads)};
    }
};

template <typename InputIterator, typename PayloadEncodingFn>
auto encode_payload_vector(InputIterator first, InputIterator last, PayloadEncodingFn encoding_fn)
{
    return Payload_Vector_Container::make(first, last, encoding_fn);
}

template <typename Payload, typename PayloadEncodingFn>
auto encode_payload_vector(gsl::span<Payload const> values, PayloadEncodingFn encoding_fn)
{
    return encode_payload_vector(values.begin(), values.end(), encoding_fn);
}

template <typename InputIterator>
auto encode_payload_vector(InputIterator first, InputIterator last)
{
    return Payload_Vector_Container::make(first, last, [](auto str, auto out_iter) {
        std::transform(
            str.begin(), str.end(), out_iter, [](auto ch) { return static_cast<std::byte>(ch); });
    });
}

auto encode_payload_vector(gsl::span<std::string const> values)
{
    return encode_payload_vector(values.begin(), values.end());
}

template <typename T>
[[nodiscard]] static auto cast_and_shift_offset(std::byte const *ptr, std::size_t &offset) -> T
{
    offset -= sizeof(T);
    return *reinterpret_cast<const T *>(ptr + offset);
}

template <typename... T>
auto unpack_head(gsl::span<std::byte const> mem) -> std::tuple<T..., gsl::span<std::byte const>>
{
    static_assert(std::is_pod_v<T...>);
    auto offset = sizeof(std::tuple<T...>);
    auto tail = mem.subspan(offset);
    auto head = std::make_tuple(cast_and_shift_offset<T>(mem.data(), offset)...);
    return std::tuple_cat(head, std::tuple<gsl::span<std::byte const>>(tail));
}

[[nodiscard]] auto split(gsl::span<std::byte const> mem, std::size_t offset)
{
    return std::tuple(mem.first(offset), mem.subspan(offset));
}

template <typename T>
[[nodiscard]] auto cast_span(gsl::span<std::byte const> mem) -> gsl::span<T const>
{
    auto type_size = sizeof(T);
    Expects(mem.size() % type_size == 0);
    return gsl::make_span(reinterpret_cast<T const *>(mem.data()), mem.size() / type_size);
}

template <typename Payload_View = std::string_view>
class Payload_Vector {
   public:
    using size_type = detail::size_type;
    using difference_type = std::make_signed_t<size_type>;
    using payload_type = Payload_View;
    using iterator = detail::Payload_Vector_Iterator<Payload_View>;

    Payload_Vector(Payload_Vector_Container const &container)
        : offsets_(container.offsets), payloads_(container.payloads)
    {}

    Payload_Vector(gsl::span<size_type const> offsets, gsl::span<std::byte const> payloads)
        : offsets_(std::move(offsets)),
          payloads_(std::move(payloads))
    {}

    template <typename ContiguousContainer>
    [[nodiscard]]
    constexpr static auto from(ContiguousContainer&& mem) -> Payload_Vector
    {
        return from(gsl::make_span(reinterpret_cast<std::byte const *>(mem.data()), mem.size()));
    }

    [[nodiscard]]
    constexpr static auto from(gsl::span<std::byte const> mem) -> Payload_Vector
    {
        auto [length, tail] = unpack_head<size_type>(mem);
        auto [offsets, payloads] = split(tail, (length + 1u) * sizeof(size_type));
        return Payload_Vector<Payload_View>(cast_span<size_type>(offsets), payloads);
    }

    [[nodiscard]]
    constexpr auto operator[](size_type idx) const -> payload_type
    {
        return *(begin() + idx);
    }

    [[nodiscard]]
    constexpr auto begin() const -> iterator
    {
        return {offsets_.begin(), payloads_.begin()};
    }
    [[nodiscard]]
    constexpr auto end() const -> iterator
    {
        return {std::prev(offsets_.end()), payloads_.end()};
    }
    [[nodiscard]] constexpr auto cbegin() const -> iterator { return begin(); }
    [[nodiscard]] constexpr auto cend() const -> iterator { return end(); }
    [[nodiscard]]
    constexpr auto size() const -> size_type
    {
        return offsets_.size() - size_type{1};
    }

   private:
    gsl::span<size_type const> offsets_;
    gsl::span<std::byte const> payloads_;
};

} // namespace pisa

namespace std {

template <typename Payload_View>
struct iterator_traits<pisa::detail::Payload_Vector_Iterator<Payload_View>> {
    using size_type = pisa::detail::size_type;
    using difference_type = std::make_signed_t<size_type>;
    using value_type = Payload_View;
    using pointer = Payload_View;
    using reference = Payload_View;
    using iterator_category = std::random_access_iterator_tag;
};

} // namespace std
