#pragma once

#include <iterator>
#include <string_view>

#include <gsl/span>

namespace pisa {

namespace detail {

    template <typename Payload_View = std::string_view, typename Offset = std::uint32_t>
    struct Payload_Vector_Iterator {
        using size_type = Offset;
        using value_type = Payload_View;
        using difference_type = std::make_signed_t<Offset>;

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

template <typename Payload_View = std::string_view, typename Offset = std::uint32_t>
class Payload_Vector {
   public:
    using size_type = Offset;
    using difference_type = Offset;
    using payload_type = Payload_View;
    using iterator = detail::Payload_Vector_Iterator<Payload_View, Offset>;

    Payload_Vector(gsl::span<size_type const> offsets, gsl::span<std::byte const> payloads)
        : offsets_(std::move(offsets)),
          payloads_(std::move(payloads))
    {}

    [[nodiscard]]
    constexpr auto operator[](size_type idx) -> payload_type
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

template <typename Payload_View, typename Offset>
struct iterator_traits<pisa::detail::Payload_Vector_Iterator<Payload_View, Offset>> {
    using difference_type = std::make_signed_t<Offset>;
    using size_type = Offset;
    using value_type = Payload_View;
    using pointer = Payload_View;
    using reference = Payload_View;
    using iterator_category = std::random_access_iterator_tag;
};

} // namespace std
