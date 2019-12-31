#pragma once

#include <stdexcept>

#include <boost/variant.hpp>

#include "bit_vector.hpp"
#include "global_parameters.hpp"
#include "util/compiler_attribute.hpp"
#include "util/likely.hpp"
#include "v1/bit_vector.hpp"

namespace pisa::v1 {

[[nodiscard]] constexpr auto positive(std::uint64_t n) -> std::uint64_t
{
    if (n == 0) {
        throw std::logic_error("argument must be positive");
    }
    return n;
}

struct CompactEliasFano {

    struct offsets {
        offsets() {}

        offsets(uint64_t base_offset,
                uint64_t universe,
                uint64_t n,
                global_parameters const& params)
            : universe(universe),
              n(positive(n)),
              log_sampling0(params.ef_log_sampling0),
              log_sampling1(params.ef_log_sampling1),
              lower_bits(universe > n ? broadword::msb(universe / n) : 0),
              mask((uint64_t(1) << lower_bits) - 1),
              // pad with a zero on both sides as sentinels
              higher_bits_length(n + (universe >> lower_bits) + 2),
              pointer_size(ceil_log2(higher_bits_length)),
              pointers0((higher_bits_length - n) >> log_sampling0), // XXX
              pointers1(n >> log_sampling1),
              pointers0_offset(base_offset),
              pointers1_offset(pointers0_offset + pointers0 * pointer_size),
              higher_bits_offset(pointers1_offset + pointers1 * pointer_size),
              lower_bits_offset(higher_bits_offset + higher_bits_length),
              end(lower_bits_offset + n * lower_bits)
        {
        }

        uint64_t universe;
        uint64_t n;
        uint64_t log_sampling0;
        uint64_t log_sampling1;

        uint64_t lower_bits;
        uint64_t mask;
        uint64_t higher_bits_length;
        uint64_t pointer_size;
        uint64_t pointers0;
        uint64_t pointers1;

        uint64_t pointers0_offset;
        uint64_t pointers1_offset;
        uint64_t higher_bits_offset;
        uint64_t lower_bits_offset;
        uint64_t end;
    };

    static PISA_FLATTEN_FUNC uint64_t bitsize(global_parameters const& params,
                                              uint64_t universe,
                                              uint64_t n)
    {
        return offsets(0, universe, n, params).end;
    }

    template <typename Iterator>
    static void write(bit_vector_builder& bvb,
                      Iterator begin,
                      uint64_t universe,
                      uint64_t n,
                      global_parameters const& params)
    {
        uint64_t base_offset = bvb.size();
        offsets of(base_offset, universe, n, params);
        // initialize all the bits to 0
        bvb.zero_extend(of.end - base_offset);

        uint64_t sample1_mask = (uint64_t(1) << of.log_sampling1) - 1;
        uint64_t offset;

        // utility function to set 0 pointers
        auto set_ptr0s = [&](uint64_t begin, uint64_t end, uint64_t rank_end) {
            uint64_t begin_zeros = begin - rank_end;
            uint64_t end_zeros = end - rank_end;

            for (uint64_t ptr0 = ceil_div(begin_zeros, uint64_t(1) << of.log_sampling0);
                 (ptr0 << of.log_sampling0) < end_zeros;
                 ++ptr0) {
                if (!ptr0)
                    continue;
                offset = of.pointers0_offset + (ptr0 - 1) * of.pointer_size;
                assert(offset + of.pointer_size <= of.pointers1_offset);
                bvb.set_bits(offset, (ptr0 << of.log_sampling0) + rank_end, of.pointer_size);
            }
        };

        uint64_t last = 0;
        uint64_t last_high = 0;
        Iterator it = begin;
        for (size_t i = 0; i < n; ++i) {
            uint64_t v = *it++;

            if (i && v < last) {
                throw std::runtime_error("Sequence is not sorted");
            }

            assert(v < universe);

            uint64_t high = (v >> of.lower_bits) + i + 1;
            uint64_t low = v & of.mask;

            bvb.set(of.higher_bits_offset + high, 1);

            offset = of.lower_bits_offset + i * of.lower_bits;
            assert(offset + of.lower_bits <= of.end);
            bvb.set_bits(offset, low, of.lower_bits);

            if (i && (i & sample1_mask) == 0) {
                uint64_t ptr1 = i >> of.log_sampling1;
                assert(ptr1 > 0);
                offset = of.pointers1_offset + (ptr1 - 1) * of.pointer_size;
                assert(offset + of.pointer_size <= of.higher_bits_offset);
                bvb.set_bits(offset, high, of.pointer_size);
            }

            // write pointers for the run of zeros in [last_high, high)
            set_ptr0s(last_high + 1, high, i);
            last_high = high;
            last = v;
        }

        // pointers to zeros after the last 1
        set_ptr0s(last_high + 1, of.higher_bits_length, n); // XXX
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
            : m_bv(&bv),
              m_of(offset, universe, n, params),
              m_position(size()),
              m_value(m_of.universe)
        {
        }

        value_type move(uint64_t position)
        {
            assert(position <= m_of.n);

            if (position == m_position) {
                return value();
            }

            uint64_t skip = position - m_position;
            // optimize small forward skips
            if (PISA_LIKELY(position > m_position && skip <= linear_scan_threshold)) {
                m_position = position;
                if (PISA_UNLIKELY(m_position == size())) {
                    m_value = m_of.universe;
                } else {
                    BitVector::unary_enumerator he = m_high_enumerator;
                    for (size_t i = 0; i < skip; ++i) {
                        he.next();
                    }
                    m_value = ((he.position() - m_of.higher_bits_offset - m_position - 1)
                               << m_of.lower_bits)
                              | read_low();
                    m_high_enumerator = he;
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

            uint64_t high_lower_bound = lower_bound >> m_of.lower_bits;
            uint64_t cur_high = m_value >> m_of.lower_bits;
            uint64_t high_diff = high_lower_bound - cur_high;

            if (PISA_LIKELY(lower_bound > m_value && high_diff <= linear_scan_threshold)) {
                // optimize small skips
                next_reader next_value(*this, m_position + 1);
                uint64_t val;
                do {
                    m_position += 1;
                    if (PISA_LIKELY(m_position < size())) {
                        val = next_value();
                    } else {
                        m_position = size();
                        val = m_of.universe;
                        break;
                    }
                } while (val < lower_bound);

                m_value = val;
                return value();
            } else {
                return slow_next_geq(lower_bound);
            }
        }

        uint64_t size() const { return m_of.n; }

        value_type next()
        {
            m_position += 1;
            assert(m_position <= size());

            if (PISA_LIKELY(m_position < size())) {
                m_value = read_next();
            } else {
                m_value = m_of.universe;
            }
            return value();
        }

        uint64_t prev_value() const
        {
            if (m_position == 0) {
                return 0;
            }

            uint64_t prev_high = 0;
            if (PISA_LIKELY(m_position < size())) {
                prev_high = m_bv->predecessor1(m_high_enumerator.position() - 1);
            } else {
                prev_high = m_bv->predecessor1(m_of.lower_bits_offset - 1);
            }
            prev_high -= m_of.higher_bits_offset;

            uint64_t prev_pos = m_position - 1;
            uint64_t prev_low =
                m_bv->get_word56(m_of.lower_bits_offset + prev_pos * m_of.lower_bits) & m_of.mask;
            return ((prev_high - prev_pos - 1) << m_of.lower_bits) | prev_low;
        }

        uint64_t position() const { return m_position; }

        inline value_type value() const { return value_type(m_position, m_value); }

       private:
        value_type PISA_NOINLINE slow_move(uint64_t position)
        {
            if (PISA_UNLIKELY(position == size())) {
                m_position = position;
                m_value = m_of.universe;
                return value();
            }

            uint64_t skip = position - m_position;
            uint64_t to_skip;
            if (position > m_position && (skip >> m_of.log_sampling1) == 0) {
                to_skip = skip - 1;
            } else {
                uint64_t ptr = position >> m_of.log_sampling1;
                uint64_t high_pos = pointer1(ptr);
                uint64_t high_rank = ptr << m_of.log_sampling1;
                m_high_enumerator =
                    BitVector::unary_enumerator(*m_bv, m_of.higher_bits_offset + high_pos);
                to_skip = position - high_rank;
            }

            m_high_enumerator.skip(to_skip);
            m_position = position;
            m_value = read_next();
            return value();
        }

        value_type PISA_NOINLINE slow_next_geq(uint64_t lower_bound)
        {
            if (PISA_UNLIKELY(lower_bound >= m_of.universe)) {
                return move(size());
            }

            uint64_t high_lower_bound = lower_bound >> m_of.lower_bits;
            uint64_t cur_high = m_value >> m_of.lower_bits;
            uint64_t high_diff = high_lower_bound - cur_high;

            // XXX bounds checking!
            uint64_t to_skip;
            if (lower_bound > m_value && (high_diff >> m_of.log_sampling0) == 0) {
                // note: at the current position in the bitvector there
                // should be a 1, but since we already consumed it, it
                // is 0 in the enumerator, so we need to skip it
                to_skip = high_diff;
            } else {
                uint64_t ptr = high_lower_bound >> m_of.log_sampling0;
                uint64_t high_pos = pointer0(ptr);
                uint64_t high_rank0 = ptr << m_of.log_sampling0;

                m_high_enumerator =
                    BitVector::unary_enumerator(*m_bv, m_of.higher_bits_offset + high_pos);
                to_skip = high_lower_bound - high_rank0;
            }

            m_high_enumerator.skip0(to_skip);
            m_position = m_high_enumerator.position() - m_of.higher_bits_offset - high_lower_bound;

            next_reader read_value(*this, m_position);
            while (true) {
                if (PISA_UNLIKELY(m_position == size())) {
                    m_value = m_of.universe;
                    return value();
                }
                auto val = read_value();
                if (val >= lower_bound) {
                    m_value = val;
                    return value();
                }
                m_position++;
            }
        }

        static const uint64_t linear_scan_threshold = 8;

        inline uint64_t read_low()
        {
            return m_bv->get_word56(m_of.lower_bits_offset + m_position * m_of.lower_bits)
                   & m_of.mask;
        }

        inline uint64_t read_next()
        {
            assert(m_position < size());
            uint64_t high = m_high_enumerator.next() - m_of.higher_bits_offset;
            return ((high - m_position - 1) << m_of.lower_bits) | read_low();
        }

        struct next_reader {
            next_reader(enumerator& e, uint64_t position)
                : e(e),
                  high_enumerator(e.m_high_enumerator),
                  high_base(e.m_of.higher_bits_offset + position + 1),
                  lower_bits(e.m_of.lower_bits),
                  lower_base(e.m_of.lower_bits_offset + position * lower_bits),
                  mask(e.m_of.mask),
                  bv(*e.m_bv)
            {
            }

            ~next_reader() { e.m_high_enumerator = high_enumerator; }

            uint64_t operator()()
            {
                uint64_t high = high_enumerator.next() - high_base;
                uint64_t low = bv.get_word56(lower_base) & mask;
                high_base += 1;
                lower_base += lower_bits;
                return (high << lower_bits) | low;
            }

            enumerator& e;
            BitVector::unary_enumerator high_enumerator;
            uint64_t high_base, lower_bits, lower_base, mask;
            BitVector const& bv;
        };

        inline uint64_t pointer(uint64_t offset, uint64_t i) const
        {
            if (i == 0) {
                return 0;
            } else {
                return m_bv->get_word56(offset + (i - 1) * m_of.pointer_size)
                       & ((uint64_t(1) << m_of.pointer_size) - 1);
            }
        }

        inline uint64_t pointer0(uint64_t i) const { return pointer(m_of.pointers0_offset, i); }

        inline uint64_t pointer1(uint64_t i) const { return pointer(m_of.pointers1_offset, i); }

        BitVector const* m_bv;
        offsets m_of;

        uint64_t m_position;
        uint64_t m_value;
        BitVector::unary_enumerator m_high_enumerator;
    };
};

struct CompactRankedBitvector {

    struct offsets {
        offsets(uint64_t base_offset,
                uint64_t universe,
                uint64_t n,
                global_parameters const& params)
            : universe(universe),
              n(n),
              log_rank1_sampling(params.rb_log_rank1_sampling),
              log_sampling1(params.rb_log_sampling1)

              ,
              rank1_sample_size(ceil_log2(n + 1)),
              pointer_size(ceil_log2(universe)),
              rank1_samples(universe >> params.rb_log_rank1_sampling),
              pointers1(n >> params.rb_log_sampling1)

              ,
              rank1_samples_offset(base_offset),
              pointers1_offset(rank1_samples_offset + rank1_samples * rank1_sample_size),
              bits_offset(pointers1_offset + pointers1 * pointer_size),
              end(bits_offset + universe)
        {
        }

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

    static PISA_FLATTEN_FUNC uint64_t bitsize(global_parameters const& params,
                                              uint64_t universe,
                                              uint64_t n)
    {
        return offsets(0, universe, n, params).end;
    }

    template <typename Iterator>
    static void write(bit_vector_builder& bvb,
                      Iterator begin,
                      uint64_t universe,
                      uint64_t n,
                      global_parameters const& params)
    {
        uint64_t base_offset = bvb.size();
        offsets of(base_offset, universe, n, params);
        // initialize all the bits to 0
        bvb.zero_extend(of.end - base_offset);

        uint64_t offset;

        auto set_rank1_samples = [&](uint64_t begin, uint64_t end, uint64_t rank) {
            for (uint64_t sample = ceil_div(begin, uint64_t(1) << of.log_rank1_sampling);
                 (sample << of.log_rank1_sampling) < end;
                 ++sample) {
                if (!sample)
                    continue;
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

        enumerator(BitVector const& bv,
                   uint64_t offset,
                   uint64_t universe,
                   uint64_t n,
                   global_parameters const& params)
            : m_bv(&bv),
              m_of(offset, universe, n, params),
              m_position(size()),
              m_value(m_of.universe)
        {
        }

        value_type move(uint64_t position)
        {
            assert(position <= size());

            if (position == m_position) {
                return value();
            }

            // optimize small forward skips
            uint64_t skip = position - m_position;
            if (PISA_LIKELY(position > m_position && skip <= linear_scan_threshold)) {
                m_position = position;
                if (PISA_UNLIKELY(m_position == size())) {
                    m_value = m_of.universe;
                } else {
                    BitVector::unary_enumerator he = m_enumerator;
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
            if (PISA_LIKELY(lower_bound > m_value && diff <= linear_scan_threshold)) {
                // optimize small skips
                BitVector::unary_enumerator he = m_enumerator;
                uint64_t val;
                do {
                    m_position += 1;
                    if (PISA_LIKELY(m_position < size())) {
                        val = he.next() - m_of.bits_offset;
                    } else {
                        m_position = size();
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

            if (PISA_LIKELY(m_position < size())) {
                m_value = read_next();
            } else {
                m_value = m_of.universe;
            }
            return value();
        }

        uint64_t size() const { return m_of.n; }

        uint64_t prev_value() const
        {
            if (m_position == 0) {
                return 0;
            }

            uint64_t pos = 0;
            if (PISA_LIKELY(m_position < size())) {
                pos = m_bv->predecessor1(m_enumerator.position() - 1);
            } else {
                pos = m_bv->predecessor1(m_of.end - 1);
            }

            return pos - m_of.bits_offset;
        }

       private:
        value_type PISA_NOINLINE slow_move(uint64_t position)
        {
            uint64_t skip = position - m_position;
            if (PISA_UNLIKELY(position == size())) {
                m_position = position;
                m_value = m_of.universe;
                return value();
            }

            uint64_t to_skip;
            if (position > m_position && (skip >> m_of.log_sampling1) == 0) {
                to_skip = skip - 1;
            } else {
                uint64_t ptr = position >> m_of.log_sampling1;
                uint64_t ptr_pos = pointer1(ptr);

                m_enumerator = BitVector::unary_enumerator(*m_bv, m_of.bits_offset + ptr_pos);
                to_skip = position - (ptr << m_of.log_sampling1);
            }

            m_enumerator.skip(to_skip);
            m_position = position;
            m_value = read_next();

            return value();
        }

        value_type PISA_NOINLINE slow_next_geq(uint64_t lower_bound)
        {
            using broadword::popcount;

            if (PISA_UNLIKELY(lower_bound >= m_of.universe)) {
                return move(size());
            }

            uint64_t skip = lower_bound - m_value;
            m_enumerator = BitVector::unary_enumerator(*m_bv, m_of.bits_offset + lower_bound);

            uint64_t begin;
            if (lower_bound > m_value && (skip >> m_of.log_rank1_sampling) == 0) {
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
            uint64_t word = (m_bv->data()[begin_word] >> begin_shift) << begin_shift;

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

        inline value_type value() const { return value_type(m_position, m_value); }

        inline uint64_t read_next() { return m_enumerator.next() - m_of.bits_offset; }

        inline uint64_t pointer(uint64_t offset, uint64_t i, uint64_t size) const
        {
            if (i == 0) {
                return 0;
            } else {
                return m_bv->get_word56(offset + (i - 1) * size) & ((uint64_t(1) << size) - 1);
            }
        }

        inline uint64_t pointer1(uint64_t i) const
        {
            return pointer(m_of.pointers1_offset, i, m_of.pointer_size);
        }

        inline uint64_t rank1_sample(uint64_t i) const
        {
            return pointer(m_of.rank1_samples_offset, i, m_of.rank1_sample_size);
        }

        BitVector const* m_bv;
        offsets m_of;

        uint64_t m_position;
        uint64_t m_value;
        BitVector::unary_enumerator m_enumerator;
    };
};

struct AllOnesSequence {

    inline static uint64_t bitsize(global_parameters const& /* params */,
                                   uint64_t universe,
                                   uint64_t n)
    {
        return (universe == n) ? 0 : uint64_t(-1);
    }

    template <typename Iterator>
    static void write(
        bit_vector_builder&, Iterator, uint64_t universe, uint64_t n, global_parameters const&)
    {
        assert(universe == n);
        (void)universe;
        (void)n;
    }

    class enumerator {
       public:
        typedef std::pair<uint64_t, uint64_t> value_type; // (position, value)

        enumerator(
            BitVector const&, uint64_t, uint64_t universe, uint64_t n, global_parameters const&)
            : m_universe(universe), m_position(size())
        {
            assert(universe == n);
            (void)n;
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

        uint64_t size() const { return m_universe; }

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

struct IndexedSequence {

    enum index_type {
        elias_fano = 0,
        ranked_bitvector = 1,
        all_ones = 2,

        index_types = 3
    };

    static const uint64_t type_bits = 1; // all_ones is implicit

    static PISA_FLATTEN_FUNC uint64_t bitsize(global_parameters const& params,
                                              uint64_t universe,
                                              uint64_t n)
    {
        uint64_t best_cost = AllOnesSequence::bitsize(params, universe, n);

        uint64_t ef_cost = CompactEliasFano::bitsize(params, universe, n) + type_bits;
        if (ef_cost < best_cost) {
            best_cost = ef_cost;
        }

        uint64_t rb_cost = CompactRankedBitvector::bitsize(params, universe, n) + type_bits;
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
        uint64_t best_cost = AllOnesSequence::bitsize(params, universe, n);
        int best_type = all_ones;

        if (best_cost) {
            uint64_t ef_cost = CompactEliasFano::bitsize(params, universe, n) + type_bits;
            if (ef_cost < best_cost) {
                best_cost = ef_cost;
                best_type = elias_fano;
            }

            uint64_t rb_cost = CompactRankedBitvector::bitsize(params, universe, n) + type_bits;
            if (rb_cost < best_cost) {
                best_cost = rb_cost;
                best_type = ranked_bitvector;
            }

            bvb.append_bits(best_type, type_bits);
        }

        switch (best_type) {
        case elias_fano:
            CompactEliasFano::write(bvb, begin, universe, n, params);
            break;
        case ranked_bitvector:
            CompactRankedBitvector::write(bvb, begin, universe, n, params);
            break;
        case all_ones:
            AllOnesSequence::write(bvb, begin, universe, n, params);
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
            : m_universe(universe)
        {
            if (AllOnesSequence::bitsize(params, universe, n) == 0) {
                m_type = all_ones;
            } else {
                m_type = index_type(bv.get_word56(offset) & ((uint64_t(1) << type_bits) - 1));
            }

            switch (m_type) {
            case elias_fano:
                m_enumerator =
                    CompactEliasFano::enumerator(bv, offset + type_bits, universe, n, params);
                break;
            case ranked_bitvector:
                m_enumerator =
                    CompactRankedBitvector::enumerator(bv, offset + type_bits, universe, n, params);
                break;
            case all_ones:
                m_enumerator =
                    AllOnesSequence::enumerator(bv, offset + type_bits, universe, n, params);
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
        value_type next_geq(uint64_t lower_bound)
        {
            return boost::apply_visitor(
                [&lower_bound](auto&& e) { return e.next_geq(lower_bound); }, m_enumerator);
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

        [[nodiscard]] auto universe() const -> std::uint64_t { return m_universe; }

       private:
        index_type m_type;
        boost::variant<CompactEliasFano::enumerator,
                       CompactRankedBitvector::enumerator,
                       AllOnesSequence::enumerator>
            m_enumerator;
        std::uint32_t m_universe;
    };
};

} // namespace pisa::v1
