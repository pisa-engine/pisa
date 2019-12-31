#pragma once

#include "tbb/task_group.h"
#include <stdexcept>

#include "codec/integer_codes.hpp"
#include "configuration.hpp"
#include "global_parameters.hpp"
#include "optimal_partition.hpp"
#include "util/util.hpp"
#include "v1/bit_vector.hpp"
#include "v1/sequence/indexed_sequence.hpp"

namespace pisa::v1 {

template <typename BaseSequence = IndexedSequence>
struct PartitionedSequence {

    using base_sequence_type = BaseSequence;
    using base_sequence_enumerator = typename base_sequence_type::enumerator;

    template <typename Iterator>
    static void write(bit_vector_builder& bvb,
                      Iterator begin,
                      std::uint64_t universe,
                      std::uint64_t n,
                      global_parameters const& params)
    {
        assert(n > 0);
        auto partition = compute_partition(begin, universe, n, params);

        size_t partitions = partition.size();
        assert(partitions > 0);
        assert(partition.front() != 0);
        assert(partition.back() == n);
        write_gamma_nonzero(bvb, partitions);

        std::vector<std::uint64_t> cur_partition;
        std::uint64_t cur_base = 0;
        if (partitions == 1) {
            cur_base = *begin;
            Iterator it = begin;

            for (size_t i = 0; i < n; ++i, ++it) {
                cur_partition.push_back(*it - cur_base);
            }

            std::uint64_t universe_bits = ceil_log2(universe);
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
            std::vector<std::uint64_t> endpoints;
            std::vector<std::uint64_t> upper_bounds;

            std::uint64_t cur_i = 0;
            Iterator it = begin;
            cur_base = *begin;
            upper_bounds.push_back(cur_base);

            for (size_t p = 0; p < partition.size(); ++p) {
                cur_partition.clear();
                std::uint64_t value = 0;
                for (; cur_i < partition[p]; ++cur_i, ++it) {
                    value = *it;
                    cur_partition.push_back(value - cur_base);
                }

                std::uint64_t upper_bound = value;
                assert(cur_partition.size() > 0);
                base_sequence_type::write(bv_sequences,
                                          cur_partition.begin(),
                                          cur_partition.back() + 1,
                                          cur_partition.size(), // XXX skip last one?
                                          params);
                endpoints.push_back(bv_sequences.size());
                upper_bounds.push_back(upper_bound);
                cur_base = upper_bound + 1;
            }

            bit_vector_builder bv_sizes;
            CompactEliasFano::write(bv_sizes, partition.begin(), n, partitions - 1, params);

            bit_vector_builder bv_upper_bounds;
            CompactEliasFano::write(
                bv_upper_bounds, upper_bounds.begin(), universe, partitions + 1, params);

            std::uint64_t endpoint_bits = ceil_log2(bv_sequences.size() + 1);
            write_gamma(bvb, endpoint_bits);

            bvb.append(bv_sizes);
            bvb.append(bv_upper_bounds);

            for (std::uint64_t p = 0; p < endpoints.size() - 1; ++p) {
                bvb.append_bits(endpoints[p], endpoint_bits);
            }

            bvb.append(bv_sequences);
        }
    }

    class enumerator {
       public:
        using value_type = std::pair<std::uint64_t, std::uint64_t>; // (position, value)

        enumerator(BitVector const& bv,
                   std::uint64_t offset,
                   std::uint64_t universe,
                   std::uint64_t n,
                   global_parameters const& params)
            : m_params(params), m_size(n), m_universe(universe), m_bv(&bv)
        {
            BitVector::enumerator it(bv, offset);
            m_partitions = read_gamma_nonzero(it);
            if (m_partitions == 1) {
                m_cur_partition = 0;
                m_cur_begin = 0;
                m_cur_end = n;

                std::uint64_t universe_bits = ceil_log2(universe);
                m_cur_base = it.take(universe_bits);
                auto ub = 0;
                if (n > 1) {
                    std::uint64_t universe_delta = read_delta(it);
                    ub = universe_delta > 0U ? universe_delta : (universe - m_cur_base - 1);
                }

                m_partition_enum =
                    base_sequence_enumerator(*m_bv, it.position(), ub + 1, n, m_params);

                m_cur_upper_bound = m_cur_base + ub;
            } else {
                m_endpoint_bits = read_gamma(it);

                std::uint64_t cur_offset = it.position();
                m_sizes =
                    CompactEliasFano::enumerator(bv, cur_offset, n, m_partitions - 1, params);
                cur_offset += CompactEliasFano::bitsize(params, n, m_partitions - 1);

                m_upper_bounds = CompactEliasFano::enumerator(
                    bv, cur_offset, universe, m_partitions + 1, params);
                cur_offset += CompactEliasFano::bitsize(params, universe, m_partitions + 1);

                m_endpoints_offset = cur_offset;
                std::uint64_t endpoints_size = m_endpoint_bits * (m_partitions - 1);
                cur_offset += endpoints_size;

                m_sequences_offset = cur_offset;
            }

            m_position = size();
            slow_move();
        }

        value_type PISA_ALWAYSINLINE move(std::uint64_t position)
        {
            assert(position <= size());
            m_position = position;

            if (m_position >= m_cur_begin && m_position < m_cur_end) {
                std::uint64_t val =
                    m_cur_base + m_partition_enum.move(m_position - m_cur_begin).second;
                return value_type(m_position, val);
            }

            return slow_move();
        }

        // note: this is instantiated oly if BaseSequence has next_geq
        template <typename Q = base_sequence_enumerator, typename = if_has_next_geq<Q>>
        value_type PISA_ALWAYSINLINE next_geq(std::uint64_t lower_bound)
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
                std::uint64_t val = m_cur_base + m_partition_enum.next().second;
                return value_type(m_position, val);
            }
            return slow_next();
        }

        [[nodiscard]] auto size() const -> std::uint64_t { return m_size; }

        [[nodiscard]] auto prev_value() const -> std::uint64_t
        {
            if (PISA_UNLIKELY(m_position == m_cur_begin)) {
                return m_cur_partition ? m_cur_base - 1 : 0;
            }
            return m_cur_base + m_partition_enum.prev_value();
        }

        [[nodiscard]] auto num_partitions() const -> std::uint64_t { return m_partitions; }
        [[nodiscard]] auto universe() const -> std::uint64_t { return m_universe; }

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
            std::uint64_t val = m_cur_base + m_partition_enum.move(0).second;
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
            auto size_it = m_sizes.next_geq(m_position + 1); // need endpoint strictly > m_position
            switch_partition(size_it.first);
            std::uint64_t val = m_cur_base + m_partition_enum.move(m_position - m_cur_begin).second;
            return value_type(m_position, val);
        }

        value_type PISA_NOINLINE slow_next_geq(std::uint64_t lower_bound)
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

        void switch_partition(std::uint64_t partition)
        {
            assert(m_partitions > 1);

            std::uint64_t endpoint =
                partition > 0U
                    ? (m_bv->get_word56(m_endpoints_offset + (partition - 1) * m_endpoint_bits)
                       & ((std::uint64_t(1) << m_endpoint_bits) - 1))
                    : 0;

            std::uint64_t partition_begin = m_sequences_offset + endpoint;
            intrinsics::prefetch(std::next(m_bv->data(), partition_begin / 64));

            m_cur_partition = partition;
            auto size_it = m_sizes.move(partition);
            m_cur_end = size_it.second;
            m_cur_begin = m_sizes.prev_value();

            auto ub_it = m_upper_bounds.move(partition + 1);
            m_cur_upper_bound = ub_it.second;
            m_cur_base = m_upper_bounds.prev_value() + (partition > 0 ? 1 : 0);

            m_partition_enum = base_sequence_enumerator(*m_bv,
                                                        partition_begin,
                                                        m_cur_upper_bound - m_cur_base + 1,
                                                        m_cur_end - m_cur_begin,
                                                        m_params);
        }

        global_parameters m_params;
        std::uint64_t m_partitions;
        std::uint64_t m_endpoints_offset;
        std::uint64_t m_endpoint_bits;
        std::uint64_t m_sequences_offset;
        std::uint64_t m_size;
        std::uint64_t m_universe;

        std::uint64_t m_position;
        std::uint64_t m_cur_partition;
        std::uint64_t m_cur_begin;
        std::uint64_t m_cur_end;
        std::uint64_t m_cur_base;
        std::uint64_t m_cur_upper_bound;

        BitVector const* m_bv;
        CompactEliasFano::enumerator m_sizes;
        CompactEliasFano::enumerator m_upper_bounds;
        base_sequence_enumerator m_partition_enum;
    };

   private:
    template <typename Iterator>
    static std::vector<uint32_t> compute_partition(Iterator begin,
                                                   std::uint64_t universe,
                                                   std::uint64_t n,
                                                   global_parameters const& params)
    {
        std::vector<uint32_t> partition;

        auto const& conf = configuration::get();

        if (base_sequence_type::bitsize(params, universe, n) < 2 * conf.fix_cost) {
            partition.push_back(n);
            return partition;
        }

        auto cost_fun = [&](std::uint64_t universe, std::uint64_t n) {
            return base_sequence_type::bitsize(params, universe, n) + conf.fix_cost;
        };

        const size_t superblock_bound = conf.eps3 != 0 ? size_t(conf.fix_cost / conf.eps3) : n;

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

            tg.run([=, &cost_fun, &conf, &superblock_partition] {
                optimal_partition opt(superblock_begin,
                                      superblock_base,
                                      superblock_universe,
                                      superblock_size,
                                      cost_fun,
                                      conf.eps1,
                                      conf.eps2);

                superblock_partition.reserve(opt.partition.size());
                for (auto& endpoint : opt.partition) {
                    superblock_partition.push_back(superblock_pos + endpoint);
                }
            });

            superblock_pos += superblock_size;
            superblock_begin = superblock_end;
            superblock_base = superblock_universe;
        }
        tg.wait();

        for (const auto& superblock_partition : superblock_partitions) {
            partition.insert(
                partition.end(), superblock_partition.begin(), superblock_partition.end());
        }

        return partition;
    }
};
} // namespace pisa::v1
