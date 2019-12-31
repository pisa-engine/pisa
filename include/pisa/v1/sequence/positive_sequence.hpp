#pragma once

#include <stdexcept>

#include <boost/variant.hpp>

#include "global_parameters.hpp"
#include "util/util.hpp"
#include "v1/bit_vector.hpp"
#include "v1/sequence/indexed_sequence.hpp"

namespace pisa::v1 {

struct StrictEliasFano {

    static PISA_FLATTEN_FUNC uint64_t bitsize(global_parameters const& params,
                                              uint64_t universe,
                                              uint64_t n)
    {
        assert(universe >= n);
        return CompactEliasFano::bitsize(params, universe - n + 1, n);
    }

    template <typename Iterator>
    static void write(bit_vector_builder& bvb,
                      Iterator begin,
                      uint64_t universe,
                      uint64_t n,
                      global_parameters const& params)
    {
        uint64_t new_universe = universe - n + 1;
        typedef typename std::iterator_traits<Iterator>::value_type value_type;
        auto new_begin = make_function_iterator(
            std::make_pair(value_type(0), begin),
            [](std::pair<value_type, Iterator>& state) {
                ++state.first;
                ++state.second;
            },
            [](std::pair<value_type, Iterator> const& state) {
                return *state.second - state.first;
            });
        CompactEliasFano::write(bvb, new_begin, new_universe, n, params);
    }

    class enumerator {
       public:
        typedef std::pair<uint64_t, uint64_t> value_type; // (position, value)

        enumerator() {}

        enumerator(BitVector const& bv,
                   uint64_t offset,
                   uint64_t universe,
                   uint64_t n,
                   global_parameters const& params)
            : m_ef_enum(bv, offset, universe - n + 1, n, params)
        {
        }

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
            if (m_ef_enum.position()) {
                return m_ef_enum.prev_value() + m_ef_enum.position() - 1;
            } else {
                return 0;
            }
        }

       private:
        CompactEliasFano::enumerator m_ef_enum;
    };
};

struct StrictSequence {

    enum index_type {
        elias_fano = 0,
        ranked_bitvector = 1,
        all_ones = 2,

        index_types = 3
    };

    static const uint64_t type_bits = 1; // all_ones is implicit

    static global_parameters strict_params(global_parameters params)
    {
        // we do not need to index the zeros
        params.ef_log_sampling0 = 63;
        params.rb_log_rank1_sampling = 63;
        return params;
    }

    static PISA_FLATTEN_FUNC uint64_t bitsize(global_parameters const& params,
                                              uint64_t universe,
                                              uint64_t n)
    {
        uint64_t best_cost = AllOnesSequence::bitsize(params, universe, n);
        auto sparams = strict_params(params);

        uint64_t ef_cost = StrictEliasFano::bitsize(sparams, universe, n) + type_bits;
        if (ef_cost < best_cost) {
            best_cost = ef_cost;
        }

        uint64_t rb_cost = CompactRankedBitvector::bitsize(sparams, universe, n) + type_bits;
        if (rb_cost < best_cost) {
            best_cost = rb_cost;
        }

        return best_cost;
    }

    template <typename Iterator>
    static void write(bit_vector_builder& bvb,
                      Iterator begin,
                      uint64_t universe,
                      uint64_t n,
                      global_parameters const& params)
    {
        auto sparams = strict_params(params);
        uint64_t best_cost = AllOnesSequence::bitsize(params, universe, n);
        int best_type = all_ones;

        if (best_cost) {
            uint64_t ef_cost = StrictEliasFano::bitsize(sparams, universe, n) + type_bits;
            if (ef_cost < best_cost) {
                best_cost = ef_cost;
                best_type = elias_fano;
            }

            uint64_t rb_cost = CompactRankedBitvector::bitsize(sparams, universe, n) + type_bits;
            if (rb_cost < best_cost) {
                best_cost = rb_cost;
                best_type = ranked_bitvector;
            }

            bvb.append_bits(best_type, type_bits);
        }

        switch (best_type) {
        case elias_fano:
            StrictEliasFano::write(bvb, begin, universe, n, sparams);
            break;
        case ranked_bitvector:
            CompactRankedBitvector::write(bvb, begin, universe, n, sparams);
            break;
        case all_ones:
            AllOnesSequence::write(bvb, begin, universe, n, sparams);
            break;
        default:
            assert(false);
        }
    }

    class enumerator {
       public:
        typedef std::pair<uint64_t, uint64_t> value_type; // (position, value)

        enumerator() {}

        enumerator(BitVector const& bv,
                   uint64_t offset,
                   uint64_t universe,
                   uint64_t n,
                   global_parameters const& params)
        {

            auto sparams = strict_params(params);

            if (AllOnesSequence::bitsize(params, universe, n) == 0) {
                m_type = all_ones;
            } else {
                m_type = index_type(bv.get_word56(offset) & ((uint64_t(1) << type_bits) - 1));
            }

            switch (m_type) {
            case elias_fano:
                m_enumerator =
                    StrictEliasFano::enumerator(bv, offset + type_bits, universe, n, sparams);
                break;
            case ranked_bitvector:
                m_enumerator = CompactRankedBitvector::enumerator(
                    bv, offset + type_bits, universe, n, sparams);
                break;
            case all_ones:
                m_enumerator =
                    AllOnesSequence::enumerator(bv, offset + type_bits, universe, n, sparams);
                break;
            default:
                throw std::invalid_argument("Unsupported type");
            }
        }

        value_type move(uint64_t position)
        {
            return boost::apply_visitor([&position](auto&& e) { return e.move(position); },
                                        m_enumerator);
        }

        value_type next()
        {
            return boost::apply_visitor([](auto&& e) { return e.next(); }, m_enumerator);
        }

        uint64_t size() const
        {
            return boost::apply_visitor([](auto&& e) { return e.size(); }, m_enumerator);
        }

        uint64_t prev_value() const
        {
            return boost::apply_visitor([](auto&& e) { return e.prev_value(); }, m_enumerator);
        }

       private:
        index_type m_type;
        boost::variant<StrictEliasFano::enumerator,
                       CompactRankedBitvector::enumerator,
                       AllOnesSequence::enumerator>
            m_enumerator;
    };
};

template <typename BaseSequence = StrictSequence>
struct PositiveSequence {

    typedef BaseSequence base_sequence_type;
    typedef typename base_sequence_type::enumerator base_sequence_enumerator;

    template <typename Iterator>
    static void write(bit_vector_builder& bvb,
                      Iterator begin,
                      uint64_t universe,
                      uint64_t n,
                      global_parameters const& params)
    {
        assert(n > 0);
        auto cumulative_begin = make_function_iterator(
            std::make_pair(uint64_t(0), begin),
            [](std::pair<uint64_t, Iterator>& state) { state.first += *state.second++; },
            [](std::pair<uint64_t, Iterator> const& state) { return state.first + *state.second; });
        base_sequence_type::write(bvb, cumulative_begin, universe, n, params);
    }

    class enumerator {
       public:
        typedef std::pair<uint64_t, uint64_t> value_type; // (position, value)

        enumerator() = delete;

        enumerator(BitVector const& bv,
                   uint64_t offset,
                   uint64_t universe,
                   uint64_t n,
                   global_parameters const& params)
            : m_base_enum(bv, offset, universe, n, params),
              m_position(m_base_enum.size()),
              m_universe(universe)
        {
        }

        value_type next() { return move(m_position + 1); }
        auto size() const { return m_base_enum.size(); }
        auto universe() const { return m_universe; }

        value_type move(uint64_t position)
        {
            // we cache m_position and m_cur to avoid the call overhead in
            // the most common cases
            uint64_t prev = m_cur;
            if (position != m_position + 1) {
                if (PISA_UNLIKELY(position == 0)) {
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

        base_sequence_enumerator const& base() const { return m_base_enum; }

       private:
        base_sequence_enumerator m_base_enum;
        uint64_t m_position;
        uint64_t m_cur{};
        uint64_t m_universe{0};
    };
};

} // namespace pisa::v1
