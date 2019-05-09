#pragma once

#include "bit_vector.hpp"
#include "util/util.hpp"

namespace pisa {

struct global_parameters;
struct all_ones_sequence {

    inline static std::uint64_t bitsize(global_parameters const & /* params */,
                                        std::uint64_t universe,
                                        std::uint64_t n)
    {
        return (universe == n) ? 0 : std::uint64_t(-1);
    }

    template <typename Iterator>
    inline static void write(bit_vector_builder &,
                             Iterator,
                             std::uint64_t universe,
                             std::uint64_t n,
                             global_parameters const &)
    {
        assert(universe == n);
        (void)universe;
        (void)n;
    }

    class enumerator {
       public:
        using value_type = std::pair<std::uint64_t, std::uint64_t>;

        inline enumerator(bit_vector const &,
                          std::uint64_t,
                          std::uint64_t universe,
                          std::uint64_t n,
                          global_parameters const &)
            : m_universe(universe), m_position(size())
        {
            assert(universe == n);
            (void)n;
        }

        value_type move(std::uint64_t position)
        {
            assert(position <= size());
            m_position = position;
            return value_type(m_position, m_position);
        }

        value_type next_geq(std::uint64_t lower_bound)
        {
            assert(lower_bound <= size());
            m_position = lower_bound;
            return value_type(m_position, m_position);
        }

        value_type next()
        {
            m_position += 1;
            return value_type(m_position, m_position);
        }

        std::uint64_t size() const { return m_universe; }

        std::uint64_t prev_value() const
        {
            if (m_position == 0) {
                return 0;
            }
            return m_position - 1;
        }

       private:
        std::uint64_t m_universe;
        std::uint64_t m_position;
    };
};
} // namespace pisa
