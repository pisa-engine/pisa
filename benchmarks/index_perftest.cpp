#include "mappable/mapper.hpp"
#include "mio/mmap.hpp"
#include "spdlog/spdlog.h"

#include "index_types.hpp"
#include "util/do_not_optimize_away.hpp"
#include "util/util.hpp"

using pisa::do_not_optimize_away;
using pisa::get_time_usecs;

template <typename IndexType, bool with_freqs>
void perftest(IndexType const& index, std::string const& type)
{
    std::string freqs_log = with_freqs ? "+freq()" : "";
    {
        size_t min_length = 4096;
        size_t max_length = 100000;
        size_t max_number_of_lists = 1000;

        spdlog::info(
            "Scanning {} posting lists with length between {} and {}",
            max_number_of_lists,
            min_length,
            max_length);

        std::vector<size_t> long_lists;
        for (size_t i = 0; i < index.size() and long_lists.size() <= max_number_of_lists; ++i) {
            if (index[i].size() >= min_length and index[i].size() < max_length) {
                long_lists.push_back(i);
            }
        }

        auto tick = get_time_usecs();
        uint64_t calls_per_list = 500000;
        size_t postings = 0;
        for (auto i: long_lists) {
            auto reader = index[i];
            auto calls = std::min(calls_per_list, reader.size());
            for (size_t i = 0; i < calls; ++i) {
                reader.next();
                do_not_optimize_away(reader.docid());
                if (with_freqs) {
                    do_not_optimize_away(reader.freq());
                }
            }
            postings += calls;
        }
        double elapsed = get_time_usecs() - tick;
        double next_ns = elapsed / postings * 1000;
        spdlog::info(
            "Performed {} next(){} in {} seconds, {:.1f} ns per posting",
            postings,
            freqs_log,
            uint64_t(elapsed / 1000000),
            next_ns);
        spdlog::info("{}\tnext{}\t{:.1f}", type, (with_freqs ? "_freq" : ""), next_ns);
    }

    uint64_t min_calls_per_list = 100;
    uint64_t max_calls_per_list = 20000;
    for (uint64_t skip = 1; skip <= 16384; skip <<= 1) {
        uint64_t min_length = min_calls_per_list * skip;
        std::vector<std::pair<size_t, std::vector<uint64_t>>> skip_values;
        for (size_t i = 0; i < index.size(); ++i) {
            auto reader = index[i];
            uint64_t size = reader.size();
            if (size < min_length) {
                continue;
            }

            skip_values.emplace_back(i, std::vector<uint64_t>());
            for (size_t i = 0; i < std::min(pisa::ceil_div(size, skip), max_calls_per_list); ++i) {
                reader.move(i * skip);
                skip_values.back().second.push_back(reader.docid());
            }
        }

        auto tick = get_time_usecs();
        size_t calls = 0;
        for (auto const& p: skip_values) {
            auto reader = index[p.first];
            for (auto const& val: p.second) {
                reader.next_geq(val);
                do_not_optimize_away(reader.docid());
                if (with_freqs) {
                    do_not_optimize_away(reader.freq());
                }
            }
            calls += p.second.size();
        }
        double elapsed = get_time_usecs() - tick;
        double next_geq_ns = elapsed / calls * 1000;

        spdlog::info(
            "Performed {} calls next_geq(){} with skip={}: {:.1f} ns per call",
            calls,
            freqs_log,
            skip,
            next_geq_ns);
        spdlog::info(
            "{}\tnext_geq{}\t{}\t{:.1f}", type, (with_freqs ? "_freq" : ""), skip, next_geq_ns);
    }
}

template <typename IndexType>
void perftest(const char* index_filename, std::string const& type)
{
    spdlog::info("Loading index from {}", index_filename);
    IndexType index;
    mio::mmap_source m(index_filename);
    pisa::mapper::map(index, m, pisa::mapper::map_flags::warmup);

    perftest<IndexType, false>(index, type);
    perftest<IndexType, true>(index, type);
}

int main(int argc, const char** argv)
{
    using namespace pisa;

    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <index type> <index filename>" << std::endl;
        return 1;
    }

    std::string type = argv[1];
    const char* index_filename = argv[2];

    if (false) {
#define LOOP_BODY(R, DATA, T)                                    \
    }                                                            \
    else if (type == BOOST_PP_STRINGIZE(T))                      \
    {                                                            \
        perftest<BOOST_PP_CAT(T, _index)>(index_filename, type); \
        /**/

        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY
    } else {
        spdlog::error("Unknown type {}", type);
    }
}
