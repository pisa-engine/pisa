#pragma once

#include <stdexcept>

#include "codec/compact_elias_fano.hpp"
#include "util/util.hpp"

namespace pisa {

struct strict_elias_fano {
    static PISA_FLATTEN_FUNC uint64_t
    bitsize(global_parameters const& params, uint64_t universe, uint64_t n)
    {
        assert(universe >= n);
        return compact_elias_fano::bitsize(params, universe - n + 1, n);
    }

    template <typename Iterator>
    static void write(
        bit_vector_builder& bvb,
        Iterator begin,
        uint64_t universe,
        uint64_t n,
        global_parameters const& params)
    {
        uint64_t new_universe = universe - n + 1;
        using value_type = typename std::iterator_traits<Iterator>::value_type;
        auto new_begin = make_function_iterator(
            std::make_pair(value_type(0), begin),
            [](std::pair<value_type, Iterator>& state) {
                ++state.first;
                ++state.second;
            },
            [](std::pair<value_type, Iterator> const& state) { return *state.second - state.first; });
        compact_elias_fano::write(bvb, new_begin, new_universe, n, params);
    }

    class enumerator {
      public:
        using value_type = std::pair<uint64_t, uint64_t>;  // (position, value)

        enumerator() = default;

        enumerator(
            bit_vector const& bv,
            uint64_t offset,
            uint64_t universe,
            uint64_t n,
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

        uint64_t size() const { return m_ef_enum.size(); }

        uint64_t prev_value() const
        {
            if (m_ef_enum.position() != 0U) {
                return m_ef_enum.prev_value() + m_ef_enum.position() - 1;
            }
            return 0;
        }

      private:
        compact_elias_fano::enumerator m_ef_enum;
    };
};
}  // namespace pisa
