#pragma once

#include <iterator>

#include "util/util.hpp"

namespace pisa {

struct Lexicon_Data {
    std::size_t size = 0;
    std::vector<char> pointers{};
    std::vector<char> payloads{};

    Lexicon_Data(std::size_t size, std::vector<char> pointers, std::vector<char> payloads)
        : size(size), pointers(std::move(pointers)), payloads(std::move(payloads))
    {}

    template <typename Iterator>
    Lexicon_Data(Iterator first, Iterator last)
    {
        auto push_pointer = [&](std::ptrdiff_t ptr) {
            pointers.insert(pointers.end(),
                            reinterpret_cast<char *>(&ptr),
                            reinterpret_cast<char *>(&ptr) + sizeof(ptr));
        };
        for (; first != last; ++first) {
            push_pointer(payloads.size());
            payloads.insert(payloads.end(), first->begin(), first->end());
            size += 1;
        }
        push_pointer(payloads.size());
    }

    std::streamsize serialize(std::ostream &os)
    {
        os.write(reinterpret_cast<char *>(&size), sizeof(size));
        os.write(&pointers[0], pointers.size());
        os.write(&payloads[0], payloads.size());
        return sizeof(size) + pointers.size() + payloads.size();
    }

    [[nodiscard]] auto serialize() const -> std::vector<char>
    {
        std::vector<char> data;
        data.insert(data.end(),
                    reinterpret_cast<char const *>(&size),
                    reinterpret_cast<char const *>(&size) + sizeof(size));
        data.insert(data.end(), pointers.begin(), pointers.end());
        data.insert(data.end(), payloads.begin(), payloads.end());
        return data;
    }
};

class Lexicon_View {
    using size_type = std::size_t;

   public:
    Lexicon_View() : size_(0), pointers_(nullptr), payloads_(nullptr) {}
    Lexicon_View(size_type size, char const *pointer_data, char const *payload_data)
        : size_(size),
          pointers_(reinterpret_cast<std::ptrdiff_t const *>(pointer_data)),
          payloads_(payload_data)
    {}
    Lexicon_View(Lexicon_View const &) = default;
    Lexicon_View(Lexicon_View &&) = default;
    Lexicon_View &operator=(Lexicon_View const &) = default;
    Lexicon_View &operator=(Lexicon_View &&) = default;

    [[nodiscard]] static auto parse(char const *data) -> Lexicon_View
    {
        size_type size = *reinterpret_cast<size_type const *>(data);
        char const *pointers = data + sizeof(size_type);
        char const *payloads = data + sizeof(size_type) + (size + 1) * sizeof(std::ptrdiff_t);
        return {size, pointers, payloads};
    }

    struct iterator {
        using difference_type = std::ptrdiff_t;
        using value_type = std::string_view;
        using iterator_category = std::random_access_iterator_tag;

        std::ptrdiff_t const *ptr = nullptr;
        char const *payloads = nullptr;

        [[nodiscard]] constexpr auto operator==(iterator const &other) const -> bool
        {
            return ptr == other.ptr;
        }

        [[nodiscard]] constexpr auto operator!=(iterator const &other) const -> bool
        {
            return ptr != other.ptr;
        }

        [[nodiscard]] constexpr auto operator+(difference_type n) const -> iterator
        {
            return iterator{std::next(ptr, n), payloads};
        }

        constexpr auto operator+=(difference_type n) -> iterator &
        {
            std::advance(ptr, n);
            return *this;
        }

        constexpr auto operator++() -> iterator &
        {
            ++ptr;
            return *this;
        }

        [[nodiscard]] constexpr auto operator++(int) -> iterator
        {
            return iterator{ptr++, payloads};
        }

        [[nodiscard]] constexpr auto operator-(iterator const &rhs) const -> difference_type
        {
            return std::distance(rhs.ptr, ptr);
        }

        [[nodiscard]] constexpr auto operator-(difference_type n) const -> iterator
        {
            return iterator{std::prev(ptr, n), payloads};
        }

        constexpr auto operator-=(difference_type n) -> iterator &
        {
            std::advance(ptr, -n);
            return *this;
        }

        constexpr auto operator--() -> iterator &
        {
            --ptr;
            return *this;
        }

        [[nodiscard]] constexpr auto operator--(int) -> iterator
        {
            return iterator{ptr--, payloads};
        }

        [[nodiscard]] constexpr auto operator*() const -> std::string_view
        {
            auto begin = std::next(payloads, *ptr);
            return std::string_view(begin, static_cast<std::size_t>(*std::next(ptr) - *ptr));
        }

        [[nodiscard]] constexpr auto operator->() const -> std::unique_ptr<std::string_view>
        {
            return std::make_unique(operator*());
        }
    };
    using const_iterator = iterator;

    [[nodiscard]] constexpr auto begin() const -> iterator
    {
        return iterator{pointers_, payloads_};
    }
    [[nodiscard]] constexpr auto rbegin() const -> iterator { return begin(); }
    [[nodiscard]] constexpr auto end() const -> iterator
    {
        return iterator{std::next(pointers_, size_), payloads_};
    }
    [[nodiscard]] constexpr auto rend() const -> iterator { return end(); }

    [[nodiscard]] constexpr auto at(size_type pos) const -> std::string_view
    {
        if (DS2I_UNLIKELY(pos >= size_)) {
            throw std::out_of_range(std::to_string(pos) +
                                    " out of range, size: " + std::to_string(size_));
        }
        return operator[](pos);
    }
    [[nodiscard]] constexpr auto operator[](size_type pos) const -> std::string_view
    {
        return *(begin() + pos);
    }
    [[nodiscard]] constexpr auto front() const -> std::string_view { return *begin(); }
    [[nodiscard]] constexpr auto back() const -> std::string_view { return *end(); }

    [[nodiscard]] constexpr auto size() const -> std::ptrdiff_t { return size_; }
    [[nodiscard]] constexpr auto empty() const -> bool { return size_ == 0; }

   private:
    size_type size_;
    std::ptrdiff_t const *pointers_;
    char const *payloads_;
};

} // namespace pisa

template <>
struct std::iterator_traits<typename pisa::Lexicon_View::iterator> {
    using difference_type = std::ptrdiff_t;
    using value_type = std::string_view;
    using pointer = void;
    using reference = std::string_view;
    using iterator_category = std::random_access_iterator_tag;
};
