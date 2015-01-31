#pragma once

#include <stdexcept>
#include <succinct/bit_vector.hpp>
#include <succinct/broadword.hpp>

#include "global_parameters.hpp"
#include "util.hpp"

namespace ds2i {

    struct compact_ranked_bitvector {

        struct offsets {
            offsets(uint64_t base_offset,
                    uint64_t universe,
                    uint64_t n,
                    global_parameters const& params)
                : universe(universe)
                , n(n)
                , log_rank1_sampling(params.rb_log_rank1_sampling)
                , log_sampling1(params.rb_log_sampling1)

                , rank1_sample_size(ceil_log2(n + 1))
                , pointer_size(ceil_log2(universe))
                , rank1_samples(universe >> params.rb_log_rank1_sampling)
                , pointers1(n >> params.rb_log_sampling1)

                , rank1_samples_offset(base_offset)
                , pointers1_offset(rank1_samples_offset + rank1_samples * rank1_sample_size)
                , bits_offset(pointers1_offset + pointers1 * pointer_size)
                , end(bits_offset + universe)
            {}

            uint64_t universe;
            uint64_t n;
            uint64_t log_rank1_sampling;
            uint64_t log_sampling1;

            uint64_t rank1_sample_size;
            uint64_t pointer_size;

            uint64_t rank1_samples;
            uint64_t pointers1;

            uint64_t rank1_samples_offset;
            uint64_t pointers1_offset;
            uint64_t bits_offset;
            uint64_t end;
        };

        static DS2I_FLATTEN_FUNC uint64_t
        bitsize(global_parameters const& params, uint64_t universe, uint64_t n)
        {
            return offsets(0, universe, n, params).end;
        }

        template <typename Iterator>
        static void write(succinct::bit_vector_builder& bvb,
                          Iterator begin,
                          uint64_t universe, uint64_t n,
                          global_parameters const& params)
        {
            using succinct::util::ceil_div;

            uint64_t base_offset = bvb.size();
            offsets of(base_offset, universe, n, params);
            // initialize all the bits to 0
            bvb.zero_extend(of.end - base_offset);

            uint64_t offset;

            auto set_rank1_samples = [&](uint64_t begin, uint64_t end,
                                         uint64_t rank) {
                for (uint64_t sample = ceil_div(begin, uint64_t(1) << of.log_rank1_sampling);
                     (sample << of.log_rank1_sampling) < end;
                     ++sample) {
                    if (!sample) continue;
                    offset = of.rank1_samples_offset + (sample - 1) * of.rank1_sample_size;
                    assert(offset + of.rank1_sample_size <= of.pointers1_offset);
                    bvb.set_bits(offset, rank, of.rank1_sample_size);
                }
            };

            uint64_t sample1_mask = (uint64_t(1) << of.log_sampling1) - 1;
            uint64_t last = 0;
            Iterator it = begin;
            for (size_t i = 0; i < n; ++i) {
                uint64_t v = *it++;
                if (i && v == last) {
                    throw std::runtime_error("Duplicate element");
                }
                if (i && v < last) {
                    throw std::runtime_error("Sequence is not sorted");
                }

                assert(!i || v > last);
                assert(v <= universe);

                bvb.set(of.bits_offset + v, 1);

                if (i && (i & sample1_mask) == 0) {
                    uint64_t ptr1 = i >> of.log_sampling1;
                    assert(ptr1 > 0);
                    offset = of.pointers1_offset + (ptr1 - 1) * of.pointer_size;
                    assert(offset + of.pointer_size <= of.bits_offset);
                    bvb.set_bits(offset, v, of.pointer_size);
                }

                set_rank1_samples(last + 1, v + 1, i);
                last = v;
            }

            set_rank1_samples(last + 1, universe, n);
        }

        class enumerator {
        public:

            typedef std::pair<uint64_t, uint64_t> value_type; // (position, value)

            enumerator(succinct::bit_vector const& bv, uint64_t offset,
                       uint64_t universe, uint64_t n,
                       global_parameters const& params)
                : m_bv(&bv)
                , m_of(offset, universe, n, params)
                , m_position(size())
                , m_value(m_of.universe)
            {}

            value_type move(uint64_t position)
            {
                assert(position <= size());

                if (position == m_position) {
                    return value();
                }

                // optimize small forward skips
                uint64_t skip = position - m_position;
                if (DS2I_LIKELY(position > m_position && skip <= linear_scan_threshold)) {
                    m_position = position;
                    if (DS2I_UNLIKELY(m_position == size())) {
                        m_value = m_of.universe;
                    } else {
                        succinct::bit_vector::unary_enumerator he = m_enumerator;
                        for (size_t i = 0; i < skip; ++i) {
                            he.next();
                        }
                        m_value = he.position() - m_of.bits_offset;
                        m_enumerator = he;
                    }

                    return value();
                }

                return slow_move(position);
            }

            value_type next_geq(uint64_t lower_bound)
            {
                if (lower_bound == m_value) {
                    return value();
                }

                uint64_t diff = lower_bound - m_value;
                if (DS2I_LIKELY(lower_bound > m_value
                           && diff <= linear_scan_threshold)) {
                    // optimize small skips
                    succinct::bit_vector::unary_enumerator he = m_enumerator;
                    uint64_t val;
                    do {
                        m_position += 1;
                        if (DS2I_LIKELY(m_position < size())) {
                            val = he.next() - m_of.bits_offset;
                        } else {
                            val = m_of.universe;
                            break;
                        }
                    } while (val < lower_bound);

                    m_value = val;
                    m_enumerator = he;
                    return value();
                } else {
                    return slow_next_geq(lower_bound);
                }
            }

            value_type next()
            {
                m_position += 1;
                assert(m_position <= size());

                if (DS2I_LIKELY(m_position < size())) {
                    m_value = read_next();
                } else {
                    m_value = m_of.universe;
                }
                return value();
            }

            uint64_t size() const
            {
                return m_of.n;
            }

            uint64_t prev_value() const
            {
                if (m_position == 0) {
                    return 0;
                }

                uint64_t pos = 0;
                if (DS2I_LIKELY(m_position < size())) {
                    pos = m_bv->predecessor1(m_enumerator.position() - 1);
                } else {
                    pos = m_bv->predecessor1(m_of.end - 1);
                }

                return pos - m_of.bits_offset;
            }

        private:

            value_type DS2I_NOINLINE slow_move(uint64_t position)
            {
                uint64_t skip = position - m_position;
                if (DS2I_UNLIKELY(position == size())) {
                    m_position = position;
                    m_value = m_of.universe;
                    return value();
                }

                uint64_t to_skip;
                if (position > m_position
                    && (skip >> m_of.log_sampling1) == 0) {
                    to_skip = skip - 1;
                } else {
                    uint64_t ptr = position >> m_of.log_sampling1;
                    uint64_t ptr_pos = pointer1(ptr);

                    m_enumerator = succinct::bit_vector::unary_enumerator
                              (*m_bv, m_of.bits_offset + ptr_pos);
                    to_skip = position - (ptr << m_of.log_sampling1);
                }

                m_enumerator.skip(to_skip);
                m_position = position;
                m_value = read_next();

                return value();
            }


            value_type DS2I_NOINLINE slow_next_geq(uint64_t lower_bound)
            {
                using succinct::broadword::popcount;

                if (DS2I_UNLIKELY(lower_bound >= m_of.universe)) {
                    return move(size());
                }

                uint64_t skip = lower_bound - m_value;
                m_enumerator = succinct::bit_vector::unary_enumerator
                    (*m_bv, m_of.bits_offset + lower_bound);

                uint64_t begin;
                if (lower_bound > m_value
                    && (skip >> m_of.log_rank1_sampling) == 0) {
                    begin = m_of.bits_offset + m_value;
                } else {
                    uint64_t block = lower_bound >> m_of.log_rank1_sampling;
                    m_position = rank1_sample(block);

                    begin = m_of.bits_offset + (block << m_of.log_rank1_sampling);
                }

                uint64_t end = m_of.bits_offset + lower_bound;
                uint64_t begin_word = begin / 64;
                uint64_t begin_shift = begin % 64;
                uint64_t end_word = end / 64;
                uint64_t end_shift = end % 64;
                uint64_t word =
                    (m_bv->data()[begin_word] >> begin_shift) << begin_shift;

                while (begin_word < end_word) {
                    m_position += popcount(word);
                    word = m_bv->data()[++begin_word];
                }
                if (end_shift) {
                    m_position += popcount(word << (64 - end_shift));
                }

                if (m_position < size()) {
                    m_value = read_next();
                } else {
                    m_value = m_of.universe;
                }

                return value();
            }


            static const uint64_t linear_scan_threshold = 8;

            inline value_type value() const
            {
                return value_type(m_position, m_value);
            }

            inline uint64_t read_next()
            {
                return m_enumerator.next() - m_of.bits_offset;
            }

            inline uint64_t pointer(uint64_t offset, uint64_t i, uint64_t size) const
            {
                if (i == 0) {
                    return 0;
                } else {
                    return
                        m_bv->get_word56(offset + (i - 1) * size)
                        & ((uint64_t(1) << size) - 1);
                }
            }

            inline uint64_t pointer1(uint64_t i) const
            {
                return pointer(m_of.pointers1_offset, i, m_of.pointer_size);
            }

            inline uint64_t rank1_sample(uint64_t i) const
            {
                return pointer(m_of.rank1_samples_offset, i,
                               m_of.rank1_sample_size);
            }

            succinct::bit_vector const* m_bv;
            offsets m_of;

            uint64_t m_position;
            uint64_t m_value;
            succinct::bit_vector::unary_enumerator m_enumerator;
        };
    };
}
