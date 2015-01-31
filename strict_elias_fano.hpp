#pragma once

#include <stdexcept>

#include "compact_elias_fano.hpp"
#include "util.hpp"

namespace ds2i {

    struct strict_elias_fano {

        static DS2I_FLATTEN_FUNC uint64_t
        bitsize(global_parameters const& params, uint64_t universe, uint64_t n)
        {
            assert(universe >= n);
            return compact_elias_fano::bitsize(params, universe - n + 1, n);
        }

        template <typename Iterator>
        static void write(succinct::bit_vector_builder& bvb,
                          Iterator begin,
                          uint64_t universe, uint64_t n,
                          global_parameters const& params)
        {
            uint64_t new_universe = universe - n + 1;
            typedef typename std::iterator_traits<Iterator>::value_type value_type;
            auto new_begin =
                make_function_iterator(std::make_pair(value_type(0), begin),
                                       [](std::pair<value_type, Iterator>& state) {
                                           ++state.first;
                                           ++state.second;
                                       }, [](std::pair<value_type, Iterator> const& state) {
                                           return *state.second - state.first;
                                       });
            compact_elias_fano::write(bvb, new_begin, new_universe, n, params);
        }

        class enumerator {
        public:

            typedef std::pair<uint64_t, uint64_t> value_type; // (position, value)

            enumerator()
            {}

            enumerator(succinct::bit_vector const& bv, uint64_t offset,
                       uint64_t universe, uint64_t n,
                       global_parameters const& params)
                : m_ef_enum(bv, offset, universe - n + 1, n, params)
            {}

            value_type move(uint64_t position)
            {
                auto val = m_ef_enum.move(position);
                return value_type(val.first, val.second + val.first);
            }

            value_type next()
            {
                auto val = m_ef_enum.next();
                return value_type(val.first, val.second + val.first);
            }

            uint64_t size() const
            {
                return m_ef_enum.size();
            }

            uint64_t prev_value() const
            {
                if (m_ef_enum.position()) {
                    return m_ef_enum.prev_value() + m_ef_enum.position() - 1;
                } else {
                    return 0;
                }
            }

        private:
            compact_elias_fano::enumerator m_ef_enum;
        };

    };
}
