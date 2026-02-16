#pragma once

#include <cassert>
#include <chrono>

#include "util/broadword.hpp"

namespace pisa {

template <typename IntType1, typename IntType2>
inline IntType1 ceil_div(IntType1 dividend, IntType2 divisor) {
    // XXX(ot): put some static check that IntType1 >= IntType2
    auto d = IntType1(divisor);
    return IntType1(dividend + d - 1) / d;
}

template <typename T>
inline void dispose(T& t) {
    T().swap(t);
}

inline uint64_t ceil_log2(const uint64_t x) {
    assert(x > 0);
    return (x > 1) ? broadword::msb(x - 1) + 1 : 0;
}

inline double get_time_usecs() {
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
    using value_type = typename std::invoke_result_t<ValueFunctor, State>;
    using difference_type = std::ptrdiff_t;
    using pointer = value_type*;
    using reference = value_type&;

    function_iterator() = default;

    explicit function_iterator(
        State&& initial_state, AdvanceFunctor&& advance_functor, ValueFunctor&& value_functor
    )
        : m_state(std::forward<State>(initial_state)),
          m_advance_functor(std::forward<AdvanceFunctor>(advance_functor)),
          m_value_functor(std::forward<ValueFunctor>(value_functor)) {}

    friend inline void swap(function_iterator& lhs, function_iterator& rhs) {
        using std::swap;
        swap(lhs.m_state, rhs.m_state);
    }

    value_type operator*() const { return m_value_functor(m_state); }

    function_iterator& operator++() {
        m_advance_functor(m_state);
        return *this;
    }

    function_iterator operator++(int) {
        function_iterator it(*this);
        operator++();
        return it;
    }

    bool operator==(function_iterator const& other) const { return m_state == other.m_state; }

    bool operator!=(function_iterator const& other) const { return !(*this == other); }

  private:
    State m_state;
    AdvanceFunctor m_advance_functor;
    ValueFunctor m_value_functor;
};

template <typename State, typename AdvanceFunctor, typename ValueFunctor>
function_iterator<State, AdvanceFunctor, ValueFunctor> make_function_iterator(
    State&& initial_state, AdvanceFunctor&& advance_functor, ValueFunctor&& value_functor
) {
    return function_iterator<State, AdvanceFunctor, ValueFunctor>(
        std::forward<State>(initial_state),
        std::forward<AdvanceFunctor>(advance_functor),
        std::forward<ValueFunctor>(value_functor)
    );
}

}  // namespace pisa
