#pragma once

#include <cstdint>
#include <vector>

#include "util/util.hpp"

namespace pisa {

namespace detail {

    /// Returns the number of 64-bit words needed to store `n` bits.
    inline std::size_t words_for(uint64_t n) { return ceil_div(n, 64); }

}  // namespace detail

/// Contains logic for encoding a bit vector.
class bit_vector_builder {
  public:
    using bits_type = std::vector<uint64_t>;

    explicit bit_vector_builder(uint64_t size = 0, bool init = false);
    bit_vector_builder(bit_vector_builder const&) = delete;
    bit_vector_builder(bit_vector_builder&&) = delete;
    bit_vector_builder& operator=(bit_vector_builder const&) = delete;
    bit_vector_builder& operator=(bit_vector_builder&&) = delete;
    ~bit_vector_builder() = default;

    /// Reserves memory for the given number of bits.
    void reserve(uint64_t size);

    /// Appends one bit to the end of the vector.
    inline void push_back(bool b)
    {
        uint64_t pos_in_word = m_size % 64;
        if (pos_in_word == 0) {
            m_bits.push_back(0);
            m_cur_word = &m_bits.back();
        }
        *m_cur_word |= (uint64_t)b << pos_in_word;
        ++m_size;
    }

    /// Sets a bit at the position `pos` to the given value.
    inline void set(uint64_t pos, bool b)
    {
        uint64_t word = pos / 64;
        uint64_t pos_in_word = pos % 64;

        m_bits[word] &= ~(uint64_t(1) << pos_in_word);
        m_bits[word] |= uint64_t(b) << pos_in_word;
    }

    /// Overrides `len` bits, starting from `pos`, with the first `len` bits from `bits`.
    inline void set_bits(uint64_t pos, uint64_t bits, size_t len)
    {
        assert(pos + len <= size());
        // check there are no spurious bits
        assert(len == 64 || (bits >> len) == 0);
        if (len == 0U) {
            return;
        }
        uint64_t mask = (len == 64) ? uint64_t(-1) : ((uint64_t(1) << len) - 1);
        uint64_t word = pos / 64;
        uint64_t pos_in_word = pos % 64;

        m_bits[word] &= ~(mask << pos_in_word);
        m_bits[word] |= bits << pos_in_word;

        uint64_t stored = 64 - pos_in_word;
        if (stored < len) {
            m_bits[word + 1] &= ~(mask >> stored);
            m_bits[word + 1] |= bits >> stored;
        }
    }

    /// Appends the first `len` bits from `bits`.
    inline void append_bits(uint64_t bits, size_t len)
    {
        // check there are no spurious bits
        assert(len == 64 || (bits >> len) == 0);
        if (len == 0U) {
            return;
        }
        uint64_t pos_in_word = m_size % 64;
        m_size += len;
        if (pos_in_word == 0) {
            m_bits.push_back(bits);
        } else {
            *m_cur_word |= bits << pos_in_word;
            if (len > 64 - pos_in_word) {
                m_bits.push_back(bits >> (64 - pos_in_word));
            }
        }
        m_cur_word = &m_bits.back();
    }

    /// Extends the vector with n zeroes.
    inline void zero_extend(uint64_t n)
    {
        m_size += n;
        uint64_t needed = detail::words_for(m_size) - m_bits.size();
        if (needed != 0U) {
            m_bits.insert(m_bits.end(), needed, 0);
            m_cur_word = &m_bits.back();
        }
    }

    /// Extends the vector with n ones.
    inline void one_extend(uint64_t n)
    {
        while (n >= 64) {
            append_bits(uint64_t(-1), 64);
            n -= 64;
        }
        if (n != 0U) {
            append_bits(uint64_t(-1) >> (64 - n), n);
        }
    }

    /// Appends another vector builder buffer.
    void append(bit_vector_builder const& rhs);

    /// Reverses bits in place.
    void reverse();

    /// Returns a reference to the underlying data buffer.
    bits_type& move_bits()
    {
        assert(detail::words_for(m_size) == m_bits.size());
        return m_bits;
    }

    /// Returns the size of the vector in bits.
    uint64_t size() const { return m_size; }

    /// Swaps data with another vector builder.
    void swap(bit_vector_builder& other);

  private:
    /// Data buffer.
    bits_type m_bits;

    /// Size in bits.
    uint64_t m_size;

    /// Currently processed 64-bit word.
    uint64_t* m_cur_word;
};

}  // namespace pisa
