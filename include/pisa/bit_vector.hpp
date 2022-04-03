#pragma once

#include <vector>

#include "util/broadword.hpp"
#include "util/util.hpp"

#include "mappable/mappable_vector.hpp"

namespace pisa {

class bit_vector_builder;

class bit_vector {
  public:
    bit_vector() = default;

    template <class Range>
    explicit bit_vector(Range const& from)
    {
        std::vector<uint64_t> bits;
        auto const first_mask = uint64_t(1);
        uint64_t mask = first_mask;
        uint64_t cur_val = 0;
        m_size = 0;
        for (auto iter = std::begin(from); iter != std::end(from); ++iter) {
            if (*iter) {
                cur_val |= mask;
            }
            mask <<= 1;
            m_size += 1;
            if (!mask) {
                bits.push_back(cur_val);
                mask = first_mask;
                cur_val = 0;
            }
        }
        if (mask != first_mask) {
            bits.push_back(cur_val);
        }
        m_bits.steal(bits);
    }

    explicit bit_vector(bit_vector_builder* from);

    template <typename Visitor>
    void map(Visitor& visit)
    {
        visit(m_size, "m_size")(m_bits, "m_bits");
    }

    void swap(bit_vector& other)
    {
        std::swap(other.m_size, m_size);
        other.m_bits.swap(m_bits);
    }

    inline size_t size() const { return m_size; }

    inline bool operator[](uint64_t pos) const
    {
        assert(pos < m_size);
        uint64_t block = pos / 64;
        assert(block < m_bits.size());
        uint64_t shift = pos % 64;
        return ((m_bits[block] >> shift) & 1) != 0U;
    }

    inline uint64_t get_bits(uint64_t pos, uint64_t len) const
    {
        assert(pos + len <= size());
        assert(len <= 64);
        if (len == 0U) {
            return 0;
        }
        uint64_t block = pos / 64;
        uint64_t shift = pos % 64;
        uint64_t mask = std::numeric_limits<std::uint64_t>::max()
            >> (std::numeric_limits<std::uint64_t>::digits - len);
        if (shift + len <= 64) {
            return m_bits[block] >> shift & mask;
        }
        return (m_bits[block] >> shift) | (m_bits[block + 1] << (64 - shift) & mask);
    }

    // same as get_bits(pos, 64) but it can extend further size(), padding with zeros
    inline uint64_t get_word(uint64_t pos) const
    {
        assert(pos < size());
        uint64_t block = pos / 64;
        uint64_t shift = pos % 64;
        uint64_t word = m_bits[block] >> shift;
        if ((shift != 0U) && block + 1 < m_bits.size()) {
            word |= m_bits[block + 1] << (64 - shift);
        }
        return word;
    }

    // unsafe and fast version of get_word, it retrieves at least 56 bits
    inline uint64_t get_word56(uint64_t pos) const
    {
        // XXX check endianness?
        const char* ptr = reinterpret_cast<const char*>(m_bits.data());
        return *(reinterpret_cast<uint64_t const*>(ptr + pos / 8)) >> (pos % 8);
    }

    inline uint64_t predecessor0(uint64_t pos) const
    {
        assert(pos < m_size);
        uint64_t block = pos / 64;
        uint64_t shift = 64 - pos % 64 - 1;
        uint64_t word = ~m_bits[block];
        word = (word << shift) >> shift;

        unsigned long ret;
        while (broadword::msb(word, ret) == 0U) {
            assert(block);
            word = ~m_bits[--block];
        };
        return block * 64 + ret;
    }

    inline uint64_t successor0(uint64_t pos) const
    {
        assert(pos < m_size);
        uint64_t block = pos / 64;
        uint64_t shift = pos % 64;
        uint64_t word = (~m_bits[block] >> shift) << shift;

        unsigned long ret;
        while (broadword::lsb(word, ret) == 0U) {
            ++block;
            assert(block < m_bits.size());
            word = ~m_bits[block];
        };
        return block * 64 + ret;
    }

    inline uint64_t predecessor1(uint64_t pos) const
    {
        assert(pos < m_size);
        uint64_t block = pos / 64;
        uint64_t shift = 64 - pos % 64 - 1;
        uint64_t word = m_bits[block];
        word = (word << shift) >> shift;

        unsigned long ret;
        while (broadword::msb(word, ret) == 0U) {
            assert(block);
            word = m_bits[--block];
        };
        return block * 64 + ret;
    }

    inline uint64_t successor1(uint64_t pos) const
    {
        assert(pos < m_size);
        uint64_t block = pos / 64;
        uint64_t shift = pos % 64;
        uint64_t word = (m_bits[block] >> shift) << shift;

        unsigned long ret;
        while (broadword::lsb(word, ret) == 0U) {
            ++block;
            assert(block < m_bits.size());
            word = m_bits[block];
        };
        return block * 64 + ret;
    }

    mapper::mappable_vector<uint64_t> const& data() const { return m_bits; }

    struct enumerator {
        enumerator() = default;

        enumerator(bit_vector const& bv, size_t pos) : m_bv(&bv), m_pos(pos), m_buf(0), m_avail(0)
        {
            m_bv->data().prefetch(m_pos / 64);
        }

        inline bool next()
        {
            if (m_avail == 0U) {
                fill_buf();
            }
            bool b = (m_buf & 1) != 0U;
            m_buf >>= 1;
            m_avail -= 1;
            m_pos += 1;
            return b;
        }

        inline uint64_t take(size_t l)
        {
            if (m_avail < l) {
                fill_buf();
            }
            uint64_t val;
            if (l != 64) {
                val = m_buf & ((uint64_t(1) << l) - 1);
                m_buf >>= l;
            } else {
                val = m_buf;
            }
            m_avail -= l;
            m_pos += l;
            return val;
        }

        inline uint64_t skip_zeros()
        {
            uint64_t zs = 0;
            // XXX the loop may be optimized by aligning access
            while (m_buf == 0U) {
                m_pos += m_avail;
                zs += m_avail;
                m_avail = 0;
                fill_buf();
            }

            uint64_t l = broadword::lsb(m_buf);
            m_buf >>= l;
            m_buf >>= 1;
            m_avail -= l + 1;
            m_pos += l + 1;
            return zs + l;
        }

        inline uint64_t position() const { return m_pos; }

      private:
        inline void fill_buf()
        {
            m_buf = m_bv->get_word(m_pos);
            m_avail = 64;
        }

        bit_vector const* m_bv{0};
        std::uint64_t m_pos{std::numeric_limits<std::uint64_t>::max()};
        std::uint64_t m_buf{0};
        std::uint64_t m_avail{0};
    };

    struct unary_enumerator {
        unary_enumerator() : m_data(0), m_position(0), m_buf(0) {}

        unary_enumerator(bit_vector const& bv, uint64_t pos)
        {
            m_data = bv.data().data();
            m_position = pos;
            m_buf = m_data[pos / 64];
            // clear low bits
            m_buf &= uint64_t(-1) << (pos % 64);
        }

        uint64_t position() const { return m_position; }

        uint64_t next()
        {
            unsigned long pos_in_word;
            uint64_t buf = m_buf;
            while (broadword::lsb(buf, pos_in_word) == 0U) {
                m_position += 64;
                buf = m_data[m_position / 64];
            }

            m_buf = buf & (buf - 1);  // clear LSB
            m_position = (m_position & ~uint64_t(63)) + pos_in_word;
            return m_position;
        }

        // skip to the k-th one after the current position
        void skip(uint64_t k)
        {
            uint64_t skipped = 0;
            uint64_t buf = m_buf;
            uint64_t w = 0;
            while (skipped + (w = broadword::popcount(buf)) <= k) {
                skipped += w;
                m_position += 64;
                buf = m_data[m_position / 64];
            }
            assert(buf);
            uint64_t pos_in_word = broadword::select_in_word(buf, k - skipped);
            m_buf = buf & (uint64_t(-1) << pos_in_word);
            m_position = (m_position & ~uint64_t(63)) + pos_in_word;
        }

        // return the position of the k-th one after the current position.
        uint64_t skip_no_move(uint64_t k)
        {
            uint64_t position = m_position;
            uint64_t skipped = 0;
            uint64_t buf = m_buf;
            uint64_t w = 0;
            while (skipped + (w = broadword::popcount(buf)) <= k) {
                skipped += w;
                position += 64;
                buf = m_data[position / 64];
            }
            assert(buf);
            uint64_t pos_in_word = broadword::select_in_word(buf, k - skipped);
            position = (position & ~uint64_t(63)) + pos_in_word;
            return position;
        }

        // skip to the k-th zero after the current position
        void skip0(uint64_t k)
        {
            uint64_t skipped = 0;
            uint64_t pos_in_word = m_position % 64;
            uint64_t buf = ~m_buf & (uint64_t(-1) << pos_in_word);
            uint64_t w = 0;
            while (skipped + (w = broadword::popcount(buf)) <= k) {
                skipped += w;
                m_position += 64;
                buf = ~m_data[m_position / 64];
            }
            assert(buf);
            pos_in_word = broadword::select_in_word(buf, k - skipped);
            m_buf = ~buf & (uint64_t(-1) << pos_in_word);
            m_position = (m_position & ~uint64_t(63)) + pos_in_word;
        }

      private:
        uint64_t const* m_data;
        uint64_t m_position;
        uint64_t m_buf;
    };

  protected:
    size_t m_size{0};
    mapper::mappable_vector<uint64_t> m_bits;
};

}  // namespace pisa
