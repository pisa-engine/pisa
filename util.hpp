#pragma once

#include <iostream>
#include <cmath>
#include <vector>
#include <map>
#include <iomanip>
#include <locale>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <sys/time.h>
#include <sys/resource.h>

#include "succinct/broadword.hpp"

#define DS2I_LIKELY(x) __builtin_expect(!!(x), 1)
#define DS2I_UNLIKELY(x) __builtin_expect(!!(x), 0)
#define DS2I_NOINLINE __attribute__((noinline))
#define DS2I_ALWAYSINLINE __attribute__((always_inline))

#if defined(__GNUC__) && !defined(__clang__)
#    define DS2I_FLATTEN_FUNC __attribute__((always_inline,flatten))
#else
#    define DS2I_FLATTEN_FUNC DS2I_ALWAYSINLINE
#endif

namespace ds2i {

    inline uint64_t ceil_log2(const uint64_t x) {
        assert(x > 0);
        return (x > 1) ? succinct::broadword::msb(x - 1) + 1 : 0;
    }

    inline std::ostream& logger()
    {
        time_t t = std::time(nullptr);
        // XXX(ot): put_time unsupported in g++ 4.7
        // return std::cerr
        //     <<  std::put_time(std::localtime(&t), "%F %T")
        //     << ": ";
        std::locale loc;
        const std::time_put<char>& tp =
            std::use_facet<std::time_put<char>>(loc);
        const char *fmt = "%F %T";
        tp.put(std::cerr, std::cerr, ' ',
               std::localtime(&t), fmt, fmt + strlen(fmt));
        return std::cerr << ": ";
    }

    inline double get_time_usecs() {
        timeval tv;
        gettimeofday(&tv, NULL);
        return double(tv.tv_sec) * 1000000 + double(tv.tv_usec);
    }

    inline double get_user_time_usecs() {
        rusage ru;
        getrusage(RUSAGE_SELF, &ru);
        return double(ru.ru_utime.tv_sec) * 1000000 + double(ru.ru_utime.tv_usec);
    }

    // stolen from folly
    template <class T>
    inline void do_not_optimize_away(T&& datum) {
        asm volatile("" : "+r" (datum));
    }

    template<typename T>
    struct has_next_geq
    {
        template<typename Fun> struct sfinae {};
        template<typename U> static char test(sfinae<decltype(U::has_next)>);
        template<typename U> static int test(...);
        enum { value = sizeof(test<T>(0)) == sizeof(char) };
    };

    // A more powerful version of boost::function_input_iterator that also works
    // with lambdas.
    //
    // Important: the functors must be stateless, otherwise the behavior is
    // undefined.
    template <typename State, typename AdvanceFunctor, typename ValueFunctor>
    class function_iterator
        : public std::iterator<std::forward_iterator_tag,
                               typename std::result_of<ValueFunctor(State)>::type> {

    public:
        function_iterator()
        {}

        function_iterator(State initial_state)
            : m_state(initial_state)
        {}

        friend inline
        void swap(function_iterator& lhs, function_iterator& rhs)
        {
            using std::swap;
            swap(lhs.m_state, rhs.m_state);
        }

        // XXX why isn't this inherited from std::iterator?
        typedef typename std::result_of<ValueFunctor(State)>::type value_type;

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

        bool operator==(function_iterator const& other) const
        {
            return m_state == other.m_state;
        }

        bool operator!=(function_iterator const& other) const
        {
            return !(*this == other);
        }

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
        stats_line()
            : first(true)
        {
            std::cout << "{";
        }

        ~stats_line()
        {
            std::cout << "}" << std::endl;
        }

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
        void emit(const char* s) const
        {
            std::cout << '"' << s << '"';
        }

        void emit(std::string const& s) const
        {
            emit(s.c_str());
        }

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
        typename std::enable_if<Pos != 0, void>::type
        emit_tuple_helper(Tuple const& t) const
        {
            emit_tuple_helper<Tuple, Pos - 1>(t);
            std::cout << ", ";
            emit(std::get<Pos>(t));
        }

        template <typename Tuple, size_t Pos>
        typename std::enable_if<Pos == 0, void>::type
        emit_tuple_helper(Tuple const& t) const
        {
            emit(std::get<0>(t));
        }

        template <typename ...Tp>
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

        bool first;
    };

}
