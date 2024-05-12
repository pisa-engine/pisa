#pragma once

#include <array>

#include "FastPFor/headers/optpfor.h"
#include "FastPFor/headers/variablebyte.h"

#include "VarIntG8IU.h"
#include "interpolative_coding.hpp"
#include "util/compiler_attribute.hpp"
#include "util/likely.hpp"
#include "util/util.hpp"

namespace pisa {

// workaround: VariableByte::decodeArray needs the buffer size, while we
// only know the number of values. It also pads to 32 bits. We need to
// rewrite
class TightVariableByte {
  public:
    template <uint32_t i>
    static uint8_t extract7bits(const uint32_t val) {
        return static_cast<uint8_t>((val >> (7 * i)) & ((1U << 7) - 1));
    }

    template <uint32_t i>
    static uint8_t extract7bitsmaskless(const uint32_t val) {
        return static_cast<uint8_t>((val >> (7 * i)));
    }

    static void encode(const uint32_t* in, const size_t length, uint8_t* out, size_t& nvalue) {
        uint8_t* bout = out;
        for (size_t k = 0; k < length; ++k) {
            const uint32_t val(in[k]);
            /**
             * Code below could be shorter. Whether it could be faster
             * depends on your compiler and machine.
             */
            if (val < (1U << 7)) {
                *bout = static_cast<uint8_t>(val | (1U << 7));
                ++bout;
            } else if (val < (1U << 14)) {
                *bout = extract7bits<0>(val);
                ++bout;
                *bout = extract7bitsmaskless<1>(val) | (1U << 7);
                ++bout;
            } else if (val < (1U << 21)) {
                *bout = extract7bits<0>(val);
                ++bout;
                *bout = extract7bits<1>(val);
                ++bout;
                *bout = extract7bitsmaskless<2>(val) | (1U << 7);
                ++bout;
            } else if (val < (1U << 28)) {
                *bout = extract7bits<0>(val);
                ++bout;
                *bout = extract7bits<1>(val);
                ++bout;
                *bout = extract7bits<2>(val);
                ++bout;
                *bout = extract7bitsmaskless<3>(val) | (1U << 7);
                ++bout;
            } else {
                *bout = extract7bits<0>(val);
                ++bout;
                *bout = extract7bits<1>(val);
                ++bout;
                *bout = extract7bits<2>(val);
                ++bout;
                *bout = extract7bits<3>(val);
                ++bout;
                *bout = extract7bitsmaskless<4>(val) | (1U << 7);
                ++bout;
            }
        }
        nvalue = bout - out;
    }

    static void encode_single(uint32_t val, std::vector<uint8_t>& out) {
        uint8_t buf[5];
        size_t nvalue;
        encode(&val, 1, buf, nvalue);
        out.insert(out.end(), buf, buf + nvalue);
    }

    static uint8_t const* decode(const uint8_t* in, uint32_t* out, size_t n) {
        const uint8_t* inbyte = in;
        for (size_t i = 0; i < n; ++i) {
            unsigned int shift = 0;
            for (uint32_t v = 0;; shift += 7) {
                uint8_t c = *inbyte++;
                v += ((c & 127) << shift);
                if ((c & 128) != 0) {
                    *out++ = v;
                    break;
                }
            }
        }
        return inbyte;
    }

    static void decode(const uint8_t* in, uint32_t* out, size_t len, size_t& n) {
        const uint8_t* inbyte = in;
        while (inbyte < in + len) {
            unsigned int shift = 0;
            for (uint32_t v = 0;; shift += 7) {
                uint8_t c = *inbyte++;
                v += ((c & 127) << shift);
                if ((c & 128) != 0) {
                    *out++ = v;
                    n += 1;
                    break;
                }
            }
        }
    }
};

struct interpolative_block {
    static constexpr std::uint64_t block_size = 128;

    static void
    encode(uint32_t const* in, uint32_t sum_of_values, size_t n, std::vector<uint8_t>& out) {
        assert(n <= block_size);
        thread_local std::array<std::uint32_t, block_size> inbuf{};
        thread_local std::vector<uint32_t> outbuf;  // TODO: Can we use array? How long does it need
                                                    // to be?
        inbuf[0] = *in;
        for (size_t i = 1; i < n; ++i) {
            inbuf[i] = inbuf[i - 1] + in[i];
        }

        if (sum_of_values == uint32_t(-1)) {
            sum_of_values = inbuf[n - 1];
            TightVariableByte::encode_single(sum_of_values, out);
        }

        bit_writer bw(outbuf);
        bw.write_interpolative(inbuf.data(), n - 1, 0, sum_of_values);
        auto const* bufptr = reinterpret_cast<uint8_t const*>(outbuf.data());
        out.insert(out.end(), bufptr, bufptr + ceil_div(bw.size(), 8));
    }

    static uint8_t const* PISA_NOINLINE
    decode(uint8_t const* in, uint32_t* out, uint32_t sum_of_values, size_t n) {
        assert(n <= block_size);
        if (sum_of_values == std::numeric_limits<std::uint32_t>::max()) {
            in = TightVariableByte::decode(in, &sum_of_values, 1);
        }

        out[n - 1] = sum_of_values;
        size_t read_interpolative = 0;
        if (n > 1) {
            bit_reader br(in);
            br.read_interpolative(out, n - 1, 0, sum_of_values);
            for (size_t i = n - 1; i > 0; --i) {
                out[i] -= out[i - 1];
            }
            read_interpolative = ceil_div(br.position(), 8);
        }

        return in + read_interpolative;
    }
};
}  // namespace pisa
