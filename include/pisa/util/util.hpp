#pragma once

#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <locale>
#include <map>
#include <vector>

#include "util/broadword.hpp"

namespace pisa {

template <typename IntType1, typename IntType2>
inline IntType1 ceil_div(IntType1 dividend, IntType2 divisor)
{
    // XXX(ot): put some static check that IntType1 >= IntType2
    auto d = IntType1(divisor);
    return IntType1(dividend + d - 1) / d;
}

template <typename T>
inline void dispose(T& t)
{
    T().swap(t);
}

inline uint64_t ceil_log2(const uint64_t x)
{
    assert(x > 0);
    return (x > 1) ? broadword::msb(x - 1) + 1 : 0;
}

inline double get_time_usecs()
{
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
}

template <typename T>
struct has_next_geq {
    template <class, class>
    class checker;
    template <typename U>
    static std::true_type test(checker<U, decltype(std::declval<U>().next_geq(0))>*);
    template <typename U>
    static std::false_type test(...);
    static const bool value = std::is_same<std::true_type, decltype(test<T>(nullptr))>::value;
};

template <typename T>
using if_has_next_geq = std::enable_if_t<has_next_geq<T>::value>;

// A more powerful version of boost::function_input_iterator that also works
// with lambdas.
//
// Important: the functors must be stateless, otherwise the behavior is
// undefined.
template <typename State, typename AdvanceFunctor, typename ValueFunctor>
class function_iterator {
  public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = typename std::result_of<ValueFunctor(State)>::type;
    using difference_type = std::ptrdiff_t;
    using pointer = value_type*;
    using reference = value_type&;

    function_iterator() = default;

    explicit function_iterator(State initial_state) : m_state(initial_state) {}

    friend inline void swap(function_iterator& lhs, function_iterator& rhs)
    {
        using std::swap;
        swap(lhs.m_state, rhs.m_state);
    }

    value_type operator*() const
    {
        // XXX I do not know if this trick is legal for stateless lambdas,
        // but it seems to work on GCC and Clang
        return (*static_cast<ValueFunctor*>(nullptr))(m_state);
    }

    function_iterator& operator++()
    {
        (*static_cast<AdvanceFunctor*>(nullptr))(m_state);
        return *this;
    }

    function_iterator operator++(int)
    {
        function_iterator it(*this);
        operator++();
        return it;
    }

    bool operator==(function_iterator const& other) const { return m_state == other.m_state; }

    bool operator!=(function_iterator const& other) const { return !(*this == other); }

  private:
    State m_state;
};

template <typename State, typename AdvanceFunctor, typename ValueFunctor>
function_iterator<State, AdvanceFunctor, ValueFunctor>
make_function_iterator(State initial_state, AdvanceFunctor, ValueFunctor)
{
    return function_iterator<State, AdvanceFunctor, ValueFunctor>(initial_state);
}

struct stats_line {
    stats_line() { std::cout << "{"; }
    stats_line(stats_line const&) = default;
    stats_line(stats_line&&) noexcept = default;
    stats_line& operator=(stats_line const&) = default;
    stats_line& operator=(stats_line&&) noexcept = default;
    ~stats_line() { std::cout << "}" << std::endl; }

    template <typename K, typename T>
    stats_line& operator()(K const& key, T const& value)
    {
        if (!first) {
            std::cout << ", ";
        } else {
            first = false;
        }

        emit(key);
        std::cout << ": ";
        emit(value);
        return *this;
    }

    template <typename T>
    stats_line& operator()(T const& obj)
    {
        return obj.dump(*this);
    }

  private:
    template <typename T>
    void emit(T const& v) const
    {
        std::cout << v;
    }

    // XXX properly escape strings
    void emit(const char* s) const { std::cout << '"' << s << '"'; }

    void emit(std::string const& s) const { emit(s.c_str()); }

    template <typename T>
    void emit(std::vector<T> const& v) const
    {
        std::cout << "[";
        bool first = true;
        for (auto const& i: v) {
            if (first) {
                first = false;
            } else {
                std::cout << ", ";
            }
            emit(i);
        }
        std::cout << "]";
    }

    template <typename K, typename V>
    void emit(std::map<K, V> const& m) const
    {
        std::vector<std::pair<K, V>> v(m.begin(), m.end());
        emit(v);
    }

    template <typename Tuple, size_t Pos>
    typename std::enable_if<Pos != 0, void>::type emit_tuple_helper(Tuple const& t) const
    {
        emit_tuple_helper<Tuple, Pos - 1>(t);
        std::cout << ", ";
        emit(std::get<Pos>(t));
    }

    template <typename Tuple, size_t Pos>
    typename std::enable_if<Pos == 0, void>::type emit_tuple_helper(Tuple const& t) const
    {
        emit(std::get<0>(t));
    }

    template <typename... Tp>
    void emit(std::tuple<Tp...> const& t) const
    {
        std::cout << "[";
        emit_tuple_helper<std::tuple<Tp...>, sizeof...(Tp) - 1>(t);
        std::cout << "]";
    }

    template <typename T1, typename T2>
    void emit(std::pair<T1, T2> const& p) const
    {
        emit(std::make_tuple(p.first, p.second));
    }

    bool first{true};
};

}  // namespace pisa
