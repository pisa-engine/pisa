#include "bit_vector_builder.hpp"

#include <algorithm>
#include <limits>

namespace pisa {

bit_vector_builder::bit_vector_builder(uint64_t size, bool init) : m_size(size)
{
    m_bits.resize(detail::words_for(size), init ? std::numeric_limits<std::uint64_t>::max() : 0U);
    if (size != 0U) {
        m_cur_word = &m_bits.back();
        // clear padding bits
        if (init && ((size % 64) != 0U)) {
            *m_cur_word >>= 64 - (size % 64);
        }
    }
}

void bit_vector_builder::reserve(uint64_t size)
{
    m_bits.reserve(detail::words_for(size));
}

void bit_vector_builder::append(bit_vector_builder const& rhs)
{
    if (rhs.size() == 0U) {
        return;
    }

    uint64_t pos = m_bits.size();
    uint64_t shift = size() % 64;
    m_size = size() + rhs.size();
    m_bits.resize(detail::words_for(m_size));

    if (shift == 0) {  // word-aligned, easy case
        std::copy(rhs.m_bits.begin(), rhs.m_bits.end(), m_bits.begin() + ptrdiff_t(pos));
    } else {
        uint64_t* cur_word = &m_bits.front() + pos - 1;
        for (size_t i = 0; i < rhs.m_bits.size() - 1; ++i) {
            uint64_t w = rhs.m_bits[i];
            *cur_word |= w << shift;
            *++cur_word = w >> (64 - shift);
        }
        *cur_word |= rhs.m_bits.back() << shift;
        if (cur_word < &m_bits.back()) {
            *++cur_word = rhs.m_bits.back() >> (64 - shift);
        }
    }
    m_cur_word = &m_bits.back();
}

void bit_vector_builder::reverse()
{
    uint64_t shift = 64 - (size() % 64);

    uint64_t remainder = 0;
    for (auto& word: m_bits) {
        uint64_t cur_word;
        if (shift != 64) {  // this should be hoisted out
            cur_word = remainder | (word << shift);
            remainder = word >> (64 - shift);
        } else {
            cur_word = word;
        }
        word = broadword::reverse_bits(cur_word);
    }
    assert(remainder == 0);
    std::reverse(m_bits.begin(), m_bits.end());
}

void bit_vector_builder::swap(bit_vector_builder& other)
{
    m_bits.swap(other.m_bits);
    std::swap(m_size, other.m_size);
    std::swap(m_cur_word, other.m_cur_word);
}

}  // namespace pisa
