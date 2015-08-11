#pragma once

#include "global_parameters.hpp"
#include "util.hpp"

namespace ds2i {

    struct all_ones_sequence {

        inline static uint64_t
        bitsize(global_parameters const& /* params */, uint64_t universe, uint64_t n)
        {
            return (universe == n || n == 1) ? 0 : uint64_t(-1);
        }

        template <typename Iterator>
        static void write(succinct::bit_vector_builder&,
                          Iterator begin,
                          uint64_t universe, uint64_t n,
                          global_parameters const&)
        {
            assert(universe == n || n == 1); (void)universe; (void)n;
            assert(*std::next(begin, n - 1) == universe - 1); (void)begin;
        }

        class enumerator {
        public:

            typedef std::pair<uint64_t, uint64_t> value_type; // (position, value)

            enumerator(succinct::bit_vector const&, uint64_t,
                       uint64_t universe, uint64_t n,
                       global_parameters const&)
                : m_n(n)
                , m_universe(universe)
                , m_position(size())
            {
                assert(universe == n || n == 1); (void)n;
            }

            value_type move(uint64_t position)
            {
                assert(position <= size());
                m_position = position;
                return value();
            }

            value_type next_geq(uint64_t lower_bound)
            {
                assert(lower_bound <= m_universe);
                if (m_n == 1) {
                    m_position = 0;
                } else {
                    m_position = lower_bound;
                }
                return value();
            }

            value_type next()
            {
                m_position += 1;
                return value();
            }

            uint64_t size() const
            {
                return m_n;
            }

            uint64_t prev_value() const
            {
                if (m_position == 0) {
                    return 0;
                }
                if (m_n == 1) {
                    return m_universe - 1;
                }
                return m_position - 1;
            }

        private:
            value_type value() const {
                if (m_n == 1) {
                    return value_type(m_position, m_universe - 1);
                } else {
                    return value_type(m_position, m_position);
                }
            }

            uint64_t m_n;
            uint64_t m_universe;
            uint64_t m_position;
        };
    };
}
