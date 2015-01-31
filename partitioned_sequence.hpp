#pragma once

#include <stdexcept>

#include "configuration.hpp"
#include "global_parameters.hpp"
#include "compact_elias_fano.hpp"
#include "indexed_sequence.hpp"
#include "integer_codes.hpp"
#include "util.hpp"
#include "optimal_partition.hpp"

namespace ds2i {

    template <typename BaseSequence = indexed_sequence>
    struct partitioned_sequence {

        typedef BaseSequence base_sequence_type;
        typedef typename base_sequence_type::enumerator base_sequence_enumerator;

        template <typename Iterator>
        static void write(succinct::bit_vector_builder& bvb,
                          Iterator begin,
                          uint64_t universe, uint64_t n,
                          global_parameters const& params)
        {
            assert(n > 0);
            auto const& conf = configuration::get();

            auto cost_fun = [&](uint64_t universe, uint64_t n) {
                return base_sequence_type::bitsize(params, universe, n) + conf.fix_cost;
            };

            optimal_partition opt(begin, universe, n, cost_fun, conf.eps1, conf.eps2);

            size_t partitions = opt.partition.size();
            assert(partitions > 0);
            assert(opt.partition.front() != 0);
            assert(opt.partition.back() == n);
            write_gamma_nonzero(bvb, partitions);

            std::vector<uint64_t> cur_partition;
            uint64_t cur_base = 0;
            if (partitions == 1) {
                cur_base = *begin;
                Iterator it = begin;

                for (size_t i = 0; i < n; ++i, ++it) {
                    cur_partition.push_back(*it - cur_base);
                }

                uint64_t universe_bits = ceil_log2(universe);
                bvb.append_bits(cur_base, universe_bits);

                // write universe only if non-singleton and not tight
                if (n > 1) {
                    if (cur_base + cur_partition.back() + 1 == universe) {
                        // tight universe
                        write_delta(bvb, 0);
                    } else {
                        write_delta(bvb, cur_partition.back());
                    }
                }

                base_sequence_type::write(bvb, cur_partition.begin(),
                                          cur_partition.back() + 1,
                                          cur_partition.size(),
                                          params);
            } else {
                succinct::bit_vector_builder bv_sequences;
                std::vector<uint64_t> endpoints;
                std::vector<uint64_t> upper_bounds;

                uint64_t cur_i = 0;
                Iterator it = begin;
                cur_base = *begin;
                upper_bounds.push_back(cur_base);

                for (size_t p = 0; p < opt.partition.size(); ++p) {
                    cur_partition.clear();
                    uint64_t value = 0;
                    for (; cur_i < opt.partition[p]; ++cur_i, ++it) {
                        value = *it;
                        cur_partition.push_back(value - cur_base);
                    }

                    uint64_t upper_bound = value;
                    assert(cur_partition.size() > 0);
                    base_sequence_type::write(bv_sequences, cur_partition.begin(),
                                              cur_partition.back() + 1,
                                              cur_partition.size(), // XXX skip last one?
                                              params);
                    endpoints.push_back(bv_sequences.size());
                    upper_bounds.push_back(upper_bound);
                    cur_base = upper_bound + 1;
                }

                succinct::bit_vector_builder bv_sizes;
                compact_elias_fano::write(bv_sizes, opt.partition.begin(),
                                          n, partitions - 1,
                                          params);

                succinct::bit_vector_builder bv_upper_bounds;
                compact_elias_fano::write(bv_upper_bounds, upper_bounds.begin(),
                                          universe, partitions + 1,
                                          params);

                uint64_t endpoint_bits = ceil_log2(bv_sequences.size() + 1);
                write_gamma(bvb, endpoint_bits);

                bvb.append(bv_sizes);
                bvb.append(bv_upper_bounds);

                for (uint64_t p = 0; p < endpoints.size() - 1; ++p) {
                    bvb.append_bits(endpoints[p], endpoint_bits);
                }

                bvb.append(bv_sequences);
            }
        }

        class enumerator {
        public:

            typedef std::pair<uint64_t, uint64_t> value_type; // (position, value)

            enumerator()
            {}

            enumerator(succinct::bit_vector const& bv, uint64_t offset,
                       uint64_t universe, uint64_t n,
                       global_parameters const& params)
                : m_params(params)
                , m_size(n)
                , m_universe(universe)
                , m_bv(&bv)
            {
                succinct::bit_vector::enumerator it(bv, offset);
                m_partitions = read_gamma_nonzero(it);
                if (m_partitions == 1) {
                    m_cur_partition = 0;
                    m_cur_begin = 0;
                    m_cur_end = n;

                    uint64_t universe_bits = ceil_log2(universe);
                    m_cur_base = it.take(universe_bits);
                    auto ub = 0;
                    if (n > 1) {
                        uint64_t universe_delta = read_delta(it);
                        ub = universe_delta ? universe_delta : (universe - m_cur_base - 1);
                    }

                    m_partition_enum = base_sequence_enumerator
                        (*m_bv, it.position(), ub + 1, n, m_params);

                    m_cur_upper_bound = m_cur_base + ub;
                } else {
                    m_endpoint_bits = read_gamma(it);

                    uint64_t cur_offset = it.position();
                    m_sizes = compact_elias_fano::enumerator(bv, cur_offset,
                                                             n, m_partitions - 1,
                                                             params);
                    cur_offset += compact_elias_fano::bitsize(params, n,
                                                              m_partitions - 1);

                    m_upper_bounds = compact_elias_fano::enumerator(bv, cur_offset,
                                                                    universe, m_partitions + 1,
                                                                    params);
                    cur_offset += compact_elias_fano::bitsize(params, universe,
                                                              m_partitions + 1);

                    m_endpoints_offset = cur_offset;
                    uint64_t endpoints_size = m_endpoint_bits * (m_partitions - 1);
                    cur_offset += endpoints_size;

                    m_sequences_offset = cur_offset;
                }

                m_position = size();
                slow_move();
            }

            value_type DS2I_ALWAYSINLINE move(uint64_t position)
            {
                assert(position <= size());
                m_position = position;

                if (m_position >= m_cur_begin && m_position < m_cur_end) {
                    uint64_t val = m_cur_base + m_partition_enum.move(m_position - m_cur_begin).second;
                    return value_type(m_position, val);
                }

                return slow_move();
            }

            // note: this is instantiated oly if BaseSequence has next_geq
            value_type DS2I_ALWAYSINLINE next_geq(uint64_t lower_bound)
            {
                if (DS2I_LIKELY(lower_bound >= m_cur_base && lower_bound <= m_cur_upper_bound)) {
                    auto val = m_partition_enum.next_geq(lower_bound - m_cur_base);
                    m_position = m_cur_begin + val.first;
                    return value_type(m_position, m_cur_base + val.second);
                }
                return slow_next_geq(lower_bound);
            }

            value_type DS2I_ALWAYSINLINE next()
            {
                ++m_position;

                if (DS2I_LIKELY(m_position < m_cur_end)) {
                    uint64_t val = m_cur_base + m_partition_enum.next().second;
                    return value_type(m_position, val);
                }
                return slow_next();
            }

            uint64_t size() const
            {
                return m_size;
            }

            uint64_t prev_value() const
            {
                if (DS2I_UNLIKELY(m_position == m_cur_begin)) {
                    return m_cur_partition ? m_cur_base - 1 : 0;
                } else {
                    return m_cur_base + m_partition_enum.prev_value();
                }
            }

            uint64_t num_partitions() const
            {
                return m_partitions;
            }

            friend class partitioned_sequence_test;

        private:

            // the compiler does not seem smart enough to figure out that this
            // is a very unlikely condition, and inlines the move(0) inside the
            // next(), causing the code to grow. Since next is called in very
            // tight loops, on microbenchmarks this causes an improvement of
            // about 3ns on my i7 3Ghz
            value_type DS2I_NOINLINE slow_next()
            {
                if (DS2I_UNLIKELY(m_position == m_size)) {
                    assert(m_cur_partition == m_partitions - 1);
                    auto val = m_partition_enum.next();
                    assert(val.first == m_partition_enum.size()); (void)val;
                    return value_type(m_position, m_universe);
                }

                switch_partition(m_cur_partition + 1);
                uint64_t val = m_cur_base + m_partition_enum.move(0).second;
                return value_type(m_position, val);
            }

            value_type DS2I_NOINLINE slow_move()
            {
                if (m_position == size()) {
                    if (m_partitions > 1) {
                        switch_partition(m_partitions - 1);
                    }
                    m_partition_enum.move(m_partition_enum.size());
                    return value_type(m_position, m_universe);
                }
                auto size_it = m_sizes.next_geq(m_position + 1); // need endpoint strictly > m_position
                switch_partition(size_it.first);
                uint64_t val = m_cur_base + m_partition_enum.move(m_position - m_cur_begin).second;
                return value_type(m_position, val);
            }

            value_type DS2I_NOINLINE slow_next_geq(uint64_t lower_bound)
            {
                if (m_partitions == 1) {
                    if (lower_bound < m_cur_base) {
                        return move(0);
                    } else {
                        return move(size());
                    }
                }

                auto ub_it = m_upper_bounds.next_geq(lower_bound);
                if (ub_it.first == 0) {
                    return move(0);
                }

                if (ub_it.first == m_upper_bounds.size()) {
                    return move(size());
                }

                switch_partition(ub_it.first - 1);
                return next_geq(lower_bound);
            }

            void switch_partition(uint64_t partition)
            {
                assert(m_partitions > 1);

                uint64_t endpoint = partition
                    ? (m_bv->get_word56(m_endpoints_offset +
                                        (partition - 1) * m_endpoint_bits)
                       & ((uint64_t(1) << m_endpoint_bits) - 1))
                    : 0;

                uint64_t partition_begin = m_sequences_offset + endpoint;
                m_bv->data().prefetch(partition_begin / 64);

                m_cur_partition = partition;
                auto size_it = m_sizes.move(partition);
                m_cur_end = size_it.second;
                m_cur_begin = m_sizes.prev_value();

                auto ub_it = m_upper_bounds.move(partition + 1);
                m_cur_upper_bound = ub_it.second;
                m_cur_base = m_upper_bounds.prev_value() + (partition ? 1 : 0);

                m_partition_enum = base_sequence_enumerator
                    (*m_bv, partition_begin,
                     m_cur_upper_bound - m_cur_base + 1,
                     m_cur_end - m_cur_begin,
                     m_params);
            }

            global_parameters m_params;
            uint64_t m_partitions;
            uint64_t m_endpoints_offset;
            uint64_t m_endpoint_bits;
            uint64_t m_sequences_offset;
            uint64_t m_size;
            uint64_t m_universe;

            uint64_t m_position;
            uint64_t m_cur_partition;
            uint64_t m_cur_begin;
            uint64_t m_cur_end;
            uint64_t m_cur_base;
            uint64_t m_cur_upper_bound;

            succinct::bit_vector const* m_bv;
            compact_elias_fano::enumerator m_sizes;
            compact_elias_fano::enumerator m_upper_bounds;
            base_sequence_enumerator m_partition_enum;
        };
    };
}
