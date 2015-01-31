#pragma once

#include "global_parameters.hpp"
#include "strict_sequence.hpp"
#include "util.hpp"

namespace ds2i {

    template <typename BaseSequence = strict_sequence>
    struct positive_sequence {

        typedef BaseSequence base_sequence_type;
        typedef typename base_sequence_type::enumerator base_sequence_enumerator;

        template <typename Iterator>
        static void write(succinct::bit_vector_builder& bvb,
                          Iterator begin,
                          uint64_t universe, uint64_t n,
                          global_parameters const& params)
        {
            assert(n > 0);
            auto cumulative_begin =
                make_function_iterator(std::make_pair(uint64_t(*begin), begin),
                                       [](std::pair<uint64_t, Iterator>& state) {
                                           state.first += *++state.second;
                                       }, [](std::pair<uint64_t, Iterator> const& state) {
                                           return state.first;
                                       });
            base_sequence_type::write(bvb, cumulative_begin, universe, n, params);

        }

        class enumerator {
        public:

            typedef std::pair<uint64_t, uint64_t> value_type; // (position, value)

            enumerator()
            {}

            enumerator(succinct::bit_vector const& bv, uint64_t offset,
                       uint64_t universe, uint64_t n,
                       global_parameters const& params)
                : m_base_enum(bv, offset, universe, n, params)
                , m_position(m_base_enum.size())
            {}

            value_type move(uint64_t position)
            {
                // we cache m_position and m_cur to avoid the call overhead in
                // the most common cases
                uint64_t prev = m_cur;
                if (position != m_position + 1) {
                    if (DS2I_UNLIKELY(position == 0)) {
                        // we need to special-case position 0
                        m_cur = m_base_enum.move(0).second;
                        m_position = 0;
                        return value_type(m_position, m_cur);
                    }
                    prev = m_base_enum.move(position - 1).second;
                }

                m_cur = m_base_enum.next().second;
                m_position = position;
                return value_type(position, m_cur - prev);
            }

            base_sequence_enumerator const& base() const
            {
                return m_base_enum;
            }

        private:

            base_sequence_enumerator m_base_enum;
            uint64_t m_position;
            uint64_t m_cur;
        };
    };
}
