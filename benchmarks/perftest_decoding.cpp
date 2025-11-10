#include "spdlog/spdlog.h"

#include "index_types.hpp"
#include "memory_source.hpp"
#include "util/do_not_optimize_away.hpp"

#include <iostream>

using pisa::do_not_optimize_away;
using pisa::get_time_usecs;

template <typename IndexType, bool decode_freqs>
double perftest(IndexType const &index)
{
    auto start = get_time_usecs();
    for (int i = 0; i < index.size(); i++) {
        auto plist = index[i];
        // Reads elements of each posting list.
        for (size_t i = 0; i < plist.size(); ++i) {
            // Puts pointer on next posting.
            plist.next();

            // Loads docid and freq (if required) of current posting. On the other hand,
            // do_not_optimize_away() is used to avoid compiler optimizations. Otherwise,
            // the compiler may decide not to execute the following sentences because the
            // result isn't used.
            do_not_optimize_away(plist.docid());
            if (decode_freqs) {
                do_not_optimize_away(plist.freq());
            }
        }
    }
    double elapsed = get_time_usecs() - start;
    return double(elapsed / 1000); // ms.
}

int main(int argc, const char **argv)
{

    using namespace pisa;

    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <index filename> <index type>" << std::endl;
        return 1;
    }

    const char *index_filename = argv[1];
    const std::string type = argv[2];

    spdlog::info("Performing test of {} ({})", index_filename, type);

    try {
        run_for_index(type, MemorySource::mapped_file(std::string(index_filename)), [](auto&& index) {
            using IndexType = std::decay_t<decltype(index)>;

            // Executes test decoding only docids (first test is warm-up).
            spdlog::info("Decoding posting lists (only docs)...");
            perftest<IndexType, false>(index); // Warm-up
            double min_docs = perftest<IndexType, false>(index);
            for (int i = 0; i < 5; i++) {
                double t = perftest<IndexType, false>(index);
                if (t < min_docs) min_docs = t;
            }
            spdlog::info("Decoding (only docs) minimum: {} ms.", min_docs);

            // Executes test decoding docids and freqs (first test is warm-up).
            spdlog::info("Decoding posting lists (with freqs)...");
            perftest<IndexType, true>(index); // Warm-up
            double min_freqs = perftest<IndexType, true>(index);
            for (int i = 0; i < 5; i++) {
                double t = perftest<IndexType, true>(index);
                if (t < min_freqs) min_freqs = t;
            }
            spdlog::info("Decoding (with freqs) minimum: {} ms.", min_freqs);
        });
    } catch (const std::exception& e) {
        spdlog::error("Error: {}", e.what());
        return 1;
    }

    return 0;
}