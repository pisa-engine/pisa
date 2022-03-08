#pragma once

#include "tbb/task_group.h"
#include <stdexcept>

#include "codec/compact_elias_fano.hpp"
#include "codec/integer_codes.hpp"
#include "global_parameters.hpp"
#include "optimal_partition.hpp"
#include "sequence/indexed_sequence.hpp"
#include "util/util.hpp"

namespace pisa {

template <typename BaseSequence = indexed_sequence>
struct partitioned_sequence {
    using base_sequence_type = BaseSequence;
    using base_sequence_enumerator = typename base_sequence_type::enumerator;

    template <typename Iterator>
    static void write(
        bit_vector_builder& bvb,
        Iterator begin,
        uint64_t universe,
        uint64_t n,
        global_parameters const& params)
    {
        assert(n > 0);
        auto partition = compute_partition(begin, universe, n, params);

        size_t partitions = partition.size();
        assert(partitions > 0);
        assert(partition.front() != 0);
        assert(partition.back() == n);
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

            base_sequence_type::write(
                bvb, cur_partition.begin(), cur_partition.back() + 1, cur_partition.size(), params);
        } else {
            bit_vector_builder bv_sequences;
            std::vector<uint64_t> endpoints;
            std::vector<uint64_t> upper_bounds;

            uint64_t cur_i = 0;
            Iterator it = begin;
            cur_base = *begin;
            upper_bounds.push_back(cur_base);

            for (size_t p = 0; p < partition.size(); ++p) {
                cur_partition.clear();
                uint64_t value = 0;
                for (; cur_i < partition[p]; ++cur_i, ++it) {
                    value = *it;
                    cur_partition.push_back(value - cur_base);
                }

                uint64_t upper_bound = value;
                assert(not cur_partition.empty());
                base_sequence_type::write(
                    bv_sequences,
                    cur_partition.begin(),
                    cur_partition.back() + 1,
                    cur_partition.size(),  // XXX skip last one?
                    params);
                endpoints.push_back(bv_sequences.size());
                upper_bounds.push_back(upper_bound);
                cur_base = upper_bound + 1;
            }

            bit_vector_builder bv_sizes;
            compact_elias_fano::write(bv_sizes, partition.begin(), n, partitions - 1, params);

            bit_vector_builder bv_upper_bounds;
            compact_elias_fano::write(
                bv_upper_bounds, upper_bounds.begin(), universe, partitions + 1, params);

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
        using value_type = std::pair<uint64_t, uint64_t>;  // (position, value)

        enumerator() = default;

        enumerator(
            bit_vector const& bv,
            uint64_t offset,
            uint64_t universe,
            uint64_t n,
            global_parameters const& params)
            : m_params(params), m_size(n), m_universe(universe), m_bv(&bv)
        {
            bit_vector::enumerator it(bv, offset);
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
                    ub = universe_delta != 0U ? universe_delta : (universe - m_cur_base - 1);
                }

                m_partition_enum =
                    base_sequence_enumerator(*m_bv, it.position(), ub + 1, n, m_params);

                m_cur_upper_bound = m_cur_base + ub;
            } else {
                m_endpoint_bits = read_gamma(it);

                uint64_t cur_offset = it.position();
                m_sizes = compact_elias_fano::enumerator(bv, cur_offset, n, m_partitions - 1, params);
                cur_offset += compact_elias_fano::bitsize(params, n, m_partitions - 1);

                m_upper_bounds = compact_elias_fano::enumerator(
                    bv, cur_offset, universe, m_partitions + 1, params);
                cur_offset += compact_elias_fano::bitsize(params, universe, m_partitions + 1);

                m_endpoints_offset = cur_offset;
                uint64_t endpoints_size = m_endpoint_bits * (m_partitions - 1);
                cur_offset += endpoints_size;

                m_sequences_offset = cur_offset;
            }

            m_position = size();
            slow_move();
        }

        value_type PISA_ALWAYSINLINE move(uint64_t position)
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
        template <typename Q = base_sequence_enumerator, typename = if_has_next_geq<Q>>
        value_type PISA_ALWAYSINLINE next_geq(uint64_t lower_bound)
        {
            if (PISA_LIKELY(lower_bound >= m_cur_base && lower_bound <= m_cur_upper_bound)) {
                auto val = m_partition_enum.next_geq(lower_bound - m_cur_base);
                m_position = m_cur_begin + val.first;
                return value_type(m_position, m_cur_base + val.second);
            }
            return slow_next_geq(lower_bound);
        }

        value_type PISA_ALWAYSINLINE next()
        {
            ++m_position;

            if (PISA_LIKELY(m_position < m_cur_end)) {
                uint64_t val = m_cur_base + m_partition_enum.next().second;
                return value_type(m_position, val);
            }
            return slow_next();
        }

        uint64_t size() const { return m_size; }

        uint64_t prev_value() const
        {
            if (PISA_UNLIKELY(m_position == m_cur_begin)) {
                return m_cur_partition != 0U ? m_cur_base - 1 : 0;
            }
            return m_cur_base + m_partition_enum.prev_value();
        }

        uint64_t num_partitions() const { return m_partitions; }

        friend class partitioned_sequence_test;

      private:
        // the compiler does not seem smart enough to figure out that this
        // is a very unlikely condition, and inlines the move(0) inside the
        // next(), causing the code to grow. Since next is called in very
        // tight loops, on microbenchmarks this causes an improvement of
        // about 3ns on my i7 3Ghz
        value_type PISA_NOINLINE slow_next()
        {
            if (PISA_UNLIKELY(m_position == m_size)) {
                assert(m_cur_partition == m_partitions - 1);
                auto val = m_partition_enum.next();
                assert(val.first == m_partition_enum.size());
                (void)val;
                return value_type(m_position, m_universe);
            }

            switch_partition(m_cur_partition + 1);
            uint64_t val = m_cur_base + m_partition_enum.move(0).second;
            return value_type(m_position, val);
        }

        value_type PISA_NOINLINE slow_move()
        {
            if (m_position == size()) {
                if (m_partitions > 1) {
                    switch_partition(m_partitions - 1);
                }
                m_partition_enum.move(m_partition_enum.size());
                return value_type(m_position, m_universe);
            }
            auto size_it = m_sizes.next_geq(m_position + 1);  // need endpoint strictly > m_position
            switch_partition(size_it.first);
            uint64_t val = m_cur_base + m_partition_enum.move(m_position - m_cur_begin).second;
            return value_type(m_position, val);
        }

        value_type PISA_NOINLINE slow_next_geq(uint64_t lower_bound)
        {
            if (m_partitions == 1) {
                if (lower_bound < m_cur_base) {
                    return move(0);
                }
                return move(size());
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

            uint64_t endpoint = partition != 0U
                ? (m_bv->get_word56(m_endpoints_offset + (partition - 1) * m_endpoint_bits)
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
            m_cur_base = m_upper_bounds.prev_value() + (partition != 0U ? 1 : 0);

            m_partition_enum = base_sequence_enumerator(
                *m_bv,
                partition_begin,
                m_cur_upper_bound - m_cur_base + 1,
                m_cur_end - m_cur_begin,
                m_params);
        }

        global_parameters m_params;
        uint64_t m_partitions{0};
        uint64_t m_endpoints_offset{0};
        uint64_t m_endpoint_bits{0};
        uint64_t m_sequences_offset{0};
        uint64_t m_size{0};
        uint64_t m_universe{0};

        uint64_t m_position{0};
        uint64_t m_cur_partition{0};
        uint64_t m_cur_begin{0};
        uint64_t m_cur_end{0};
        uint64_t m_cur_base{0};
        uint64_t m_cur_upper_bound{0};

        bit_vector const* m_bv{nullptr};
        compact_elias_fano::enumerator m_sizes;
        compact_elias_fano::enumerator m_upper_bounds;
        base_sequence_enumerator m_partition_enum;
    };

  private:
    template <typename Iterator>
    static std::vector<uint32_t> compute_partition(
        Iterator begin,
        uint64_t universe,
        uint64_t n,
        global_parameters const& params,
        // Follwing Giuseppe Ottaviano and Rossano Venturini.
        // 2014. Partitioned Elias-Fano indexes. In Proc. SIGIR
        uint64_t fix_cost = 64,
        double eps1 = 0.03,
        double eps2 = 0.3,
        double eps3 = 0.01)
    {
        std::vector<uint32_t> partition;

        if (base_sequence_type::bitsize(params, universe, n) < 2 * fix_cost) {
            partition.push_back(n);
            return partition;
        }

        auto cost_fun = [&](uint64_t universe, uint64_t n) {
            return base_sequence_type::bitsize(params, universe, n) + fix_cost;
        };

        const size_t superblock_bound = eps3 != 0 ? size_t(fix_cost / eps3) : n;

        std::deque<std::vector<uint32_t>> superblock_partitions;
        tbb::task_group tg;

        size_t superblock_pos = 0;
        auto superblock_begin = begin;
        auto superblock_base = *begin;

        while (superblock_pos < n) {
            size_t superblock_size = std::min<size_t>(superblock_bound, n - superblock_pos);
            // If the remainder is smaller than the bound (possibly
            // empty), merge it to the current (now last) superblock.
            if (n - (superblock_pos + superblock_size) < superblock_bound) {
                superblock_size = n - superblock_pos;
            }
            auto superblock_last = std::next(superblock_begin, superblock_size - 1);
            auto superblock_end = std::next(superblock_last);

            // If this is the last superblock, its universe is the
            // list universe.
            size_t superblock_universe =
                superblock_pos + superblock_size == n ? universe : *superblock_last + 1;

            superblock_partitions.emplace_back();
            auto& superblock_partition = superblock_partitions.back();

            tg.run([=, &cost_fun, &superblock_partition] {
                optimal_partition opt(
                    superblock_begin,
                    superblock_base,
                    superblock_universe,
                    superblock_size,
                    cost_fun,
                    eps1,
                    eps2);

                superblock_partition.reserve(opt.partition.size());
                for (auto& endpoint: opt.partition) {
                    superblock_partition.push_back(superblock_pos + endpoint);
                }
            });

            superblock_pos += superblock_size;
            superblock_begin = superblock_end;
            superblock_base = superblock_universe;
        }
        tg.wait();

        for (const auto& superblock_partition: superblock_partitions) {
            partition.insert(
                partition.end(), superblock_partition.begin(), superblock_partition.end());
        }

        return partition;
    }
};
}  // namespace pisa
