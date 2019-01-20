#include <iostream>
#include <algorithm>

#include "spdlog/spdlog.h"

#include "util/util.hpp"
#include "codec/block_codecs.hpp"

int main()
{
    using namespace pisa;
    static const size_t size = interpolative_block::block_size;
    static const size_t runs = 1 << 20;

    std::vector<uint32_t> values(size);
    std::vector<uint8_t> encoded;
    for (int u = 2; u <= 1024; u *= 2) {
        std::generate(values.begin(), values.end(), [&]() { return (uint32_t)rand() % u; });
        encoded.clear();
        uint32_t sum_of_values = std::accumulate(values.begin(), values.end(), 0);
        interpolative_block::encode(values.data(), sum_of_values, values.size(), encoded);

        double tick = get_time_usecs();
        for (size_t run = 0; run < runs; ++run) {
            interpolative_block::decode(encoded.data(), values.data(), sum_of_values, values.size());
            do_not_optimize_away(values[0]);
        }

        double time = (get_time_usecs() - tick) / runs * 1000;
        spdlog::info("u = {}; time = {}", u, time);
    }
}
