#pragma once

#include "global_parameters.hpp"
#include "util.hpp"

namespace ds2i {

    struct all_ones_sequence {

        inline static uint64_t
        bitsize(global_parameters const& /* params */, uint64_t universe, uint64_t n)
        {
            return (universe == n) ? 0 : uint64_t(-1);
        }

        template <typename Iterator>
        static void write(succinct::bit_vector_builder&,
                          Iterator,
                          uint64_t universe, uint64_t n,
                          global_parameters const&)
        {
            assert(universe == n); (void)universe; (void)n;
        }

        class enumerator {
        public:

            typedef std::pair<uint64_t, uint64_t> value_type; // (position, value)

            enumerator(succinct::bit_vector const&, uint64_t,
                       uint64_t universe, uint64_t n,
                       global_parameters const&)
                : m_universe(universe)
                , m_position(size())
            {
                assert(universe == n); (void)n;
            }

            value_type move(uint64_t position)
            {
                assert(position <= size());
                m_position = position;
                return value_type(m_position, m_position);
            }

            value_type next_geq(uint64_t lower_bound)
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

            uint64_t size() const
            {
                return m_universe;
            }

            uint64_t prev_value() const
            {
                if (m_position == 0) {
                    return 0;
                }
                return m_position - 1;
            }

        private:
            uint64_t m_universe;
            uint64_t m_position;
        };
    };
}
