#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <numeric>

template <size_t Q>
class mln {
    using Table = std::array<std::array<uint8_t, Q>, Q>;

    static Table generate_compression_table(const uint32_t *in, const size_t n)
    {
        std::array<std::array<uint32_t, Q>, Q> freq_table{};

        for (int i = 0; i < n - 1; ++i) {
            uint32_t current = in[i];
            uint32_t next = in[i + 1];
            if (current <= Q and next <= Q) {
                freq_table[current - 1][next - 1] += 1;
            }
        }
        Table t{};
        for (int i = 0; i < freq_table.size(); ++i) {
            auto freq_row = freq_table[i];
            std::array<uint8_t, Q> idx;
            std::iota(idx.begin(), idx.end(), 0);
            std::stable_sort(idx.begin(), idx.end(), [&](size_t i1, size_t i2) {
                return freq_row[i1] > freq_row[i2];
            });
            for (int j = 0; j < freq_row.size(); ++j) {
                t[i][idx[j]] = j + 1;
            }
        }
        return t;
    }
    static Table generate_decompression_table(Table &t)
    {
        Table dt{};
        for (int i = 0; i < t.size(); ++i)
        {
            for (int j = 0; j < t[0].size(); ++j)
            {
                dt[i][t[i][j]-1] = j+1;
            }
        }
        return dt;
    }

   public:
    static Table encode(const uint32_t *in, const size_t n, uint32_t *out)
    {
        auto t = generate_compression_table(in, n);
        out[0] = in[0];
        for (int i = 1; i < n; ++i) {
            auto prev = in[i - 1];
            auto current = in[i];
            if (prev <= Q and current <= Q) {
                out[i] = t[prev - 1][current - 1];
            } else {
                out[i] = in[i];
            }
        }
        return generate_decompression_table(t);
    }

    static void decode(const uint32_t *in, uint32_t *out, size_t n, Table &t)
    {
        out[0] = in[0];
        auto prev = out[0];
        for (int i = 1; i < n; ++i) {
            auto current = in[i];
            if (prev <= Q and current <= Q) {
                out[i] = t[prev - 1][current - 1];
            } else {
                out[i] = in[i];
            }
            prev = out[i];
        }
    }
};
