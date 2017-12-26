#pragma once

#include <vector>
#include <stdint.h>

#include "succinct/broadword.hpp"

namespace ds2i {

    class bit_writer {
    public:
        bit_writer(std::vector<uint32_t>& buf)
            : m_buf(buf)
            , m_size(0)
            , m_cur_word(nullptr)
        {
            m_buf.clear();
        }

        void write(uint32_t bits, uint32_t len)
        {
            if (!len) return;
            uint32_t pos_in_word = m_size % 32;
            m_size += len;
            if (pos_in_word == 0) {
                m_buf.push_back(bits);
            } else {
                *m_cur_word |= bits << pos_in_word;
                if (len > 32 - pos_in_word) {
                    m_buf.push_back(bits >> (32 - pos_in_word));
                }
            }
            m_cur_word = &m_buf.back();
        }

        size_t size() const {
            return m_size;
        }

        void write_int(uint32_t val, uint32_t u)
        {
            assert(u > 0);
            assert(val < u);
            auto b = succinct::broadword::msb(u);
            uint64_t m = (uint64_t(1) << (b + 1)) - u;

            if (val < m) {
                write(val, b);
            } else {
                val += m;
                // since we use little-endian we must split the writes
                write(val >> 1, b);
                write(val & 1, 1);
            }
        }

        void write_interpolative(uint32_t const* in,
                                 size_t n,
                                 uint32_t low,
                                 uint32_t high)
        {
            if (!n) return;
            assert(low <= high);

            size_t h = n / 2;
            uint32_t val = in[h];
            write_int(val - low, high - low + 1);
            write_interpolative(in, h, low, val);
            write_interpolative(in + h + 1, n - h - 1, val, high);
        }


    private:
        std::vector<uint32_t>& m_buf;
        size_t m_size;
        uint32_t* m_cur_word;
    };

    class bit_reader {
    public:
        bit_reader(uint32_t const* in)
            : m_in(in)
            , m_avail(0)
            , m_buf(0)
            , m_pos(0)
        {}

        size_t position() const
        {
            return m_pos;
        }

        uint32_t read(uint32_t len)
        {
            if (!len) return 0;

            if (m_avail < len) {
                m_buf |= uint64_t(*m_in++) << m_avail;
                m_avail += 32;
            }
            uint32_t val = m_buf & ((uint64_t(1) << len) - 1);
            m_buf >>= len;
            m_avail -= len;
            m_pos += len;

            return val;
        }

        uint32_t read_int(uint32_t u)
        {
            assert(u > 0);
            auto b = succinct::broadword::msb(u);
            uint64_t m = (uint64_t(1) << (b + 1)) - u;

            uint32_t val = read(b);
            if (val >= m) {
                val = (val << 1) + read(1) - m;
            }

            assert(val < u);
            return val;
        }

        void read_interpolative(uint32_t* out,
                                size_t n,
                                uint32_t low,
                                uint32_t high)
        {
            assert(low <= high);
            assert(n > 0);

            size_t h = n / 2;
            uint32_t val = low + read_int(high - low + 1);
            out[h] = val;
            if (n == 1) {
                // optimization to avoid two unpredictable ifs
                return;
            }
            // the two ifs are a bit ugly but it is faster than postponing them
            if (h) {
                read_interpolative(out, h, low, val);
            }
            if (n - h - 1) {
                read_interpolative(out + h + 1, n - h - 1, val, high);
            }
        }

    private:
        uint32_t const* m_in;
        uint32_t m_avail;
        uint64_t m_buf;
        size_t m_pos;
    };



}
