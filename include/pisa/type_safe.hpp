#pragma once

#include <cstdint>
#include <functional>
#include <ostream>

namespace pisa {

template <class Tag, class T, T default_value = 0>
class Integer {
  public:
    using difference_type = T;

    Integer() : m_val(default_value) {}
    explicit Integer(T val) : m_val(val) {}
    Integer(Integer const&) = default;
    Integer(Integer&&) noexcept = default;
    Integer& operator=(Integer const&) = default;
    Integer& operator=(Integer&&) noexcept = default;
    ~Integer() = default;

    explicit operator T() const { return m_val; }
    explicit operator std::size_t() const { return static_cast<std::size_t>(m_val); }
    [[nodiscard]] T get() const { return m_val; }
    [[nodiscard]] T as_int() const { return m_val; }

    [[nodiscard]] bool operator==(Integer const& other) const { return m_val == other.m_val; }
    [[nodiscard]] bool operator!=(Integer const& other) const { return m_val != other.m_val; }
    [[nodiscard]] bool operator<(Integer const& other) const { return m_val < other.m_val; }
    [[nodiscard]] bool operator<=(Integer const& other) const { return m_val <= other.m_val; }
    [[nodiscard]] bool operator>(Integer const& other) const { return m_val > other.m_val; }
    [[nodiscard]] bool operator>=(Integer const& other) const { return m_val >= other.m_val; }

    Integer& operator++()
    {
        ++m_val;
        return *this;
    }
    Integer operator++(int) { return Integer{m_val++}; }

    [[nodiscard]] Integer operator+(T difference) const { return Integer(m_val + difference); }
    Integer& operator+=(T difference)
    {
        m_val += difference;
        return *this;
    }
    [[nodiscard]] Integer operator+(Integer const& other) const
    {
        return Integer(m_val + other.m_val);
    }
    Integer& operator+=(Integer const& other)
    {
        m_val += other.m_val;
        return *this;
    }
    Integer operator-(Integer const& other) const { return Integer(m_val - other.m_val); }
    Integer& operator-=(Integer const& other)
    {
        m_val -= other.m_val;
        return *this;
    }

  private:
    T m_val;
};

}  // namespace pisa

namespace std {

template <class Tag, class T, T default_value>
struct hash<pisa::Integer<Tag, T, default_value>> {
    constexpr auto operator()(pisa::Integer<Tag, T, default_value> const& key) const noexcept
    {
        return hash<T>{}(static_cast<T>(key));
    }
};

}  // namespace std

namespace pisa {

template <class Tag, class T, T default_value>
std::ostream& operator<<(std::ostream& os, Integer<Tag, T, default_value> id)
{
    return os << static_cast<T>(id);
}

struct document_id_tag {
};
using Document_Id = Integer<document_id_tag, std::int32_t>;
struct term_id_tag {
};
using Term_Id = Integer<term_id_tag, std::int32_t>;
struct frequency_tag {
};
using Frequency = Integer<frequency_tag, std::int32_t>;
struct shard_id_tag {
};
using Shard_Id = Integer<shard_id_tag, std::int32_t>;

namespace literals {

    inline Document_Id operator"" _d(unsigned long long n)  // NOLINT
    {
        return Document_Id(n);
    }

    inline Term_Id operator"" _t(unsigned long long n)  // NOLINT
    {
        return Term_Id(n);
    }

    inline Frequency operator"" _f(unsigned long long n)  // NOLINT
    {
        return Frequency(n);
    }

    inline Shard_Id operator"" _s(unsigned long long n)  // NOLINT
    {
        return Shard_Id(n);
    }

}  // namespace literals

}  // namespace pisa
