#include "mio/mmap.hpp"
#include "spdlog/spdlog.h"

#include "mappable/mapper.hpp"
#include "sequence/partitioned_sequence.hpp"
#include "sequence/uniform_partitioned_sequence.hpp"
#include "sequence_collection.hpp"
#include "util/do_not_optimize_away.hpp"
#include "util/util.hpp"

using pisa::do_not_optimize_away;
using pisa::get_time_usecs;

template <typename BaseSequence>
void perftest(const char* index_filename)
{
    using collection_type = pisa::sequence_collection<BaseSequence>;
    spdlog::info("Loading collection from {}", index_filename);
    collection_type coll;
    mio::mmap_source m(index_filename);
    pisa::mapper::map(coll, m, pisa::mapper::map_flags::warmup);

    if (true) {
        spdlog::info("Scanning all the posting lists");
        auto tick = get_time_usecs();
        uint64_t calls_per_list = 500000;
        size_t postings = 0;
        for (size_t i = 0; i < coll.size(); ++i) {
            auto reader = coll[i];
            auto calls = std::min(calls_per_list, reader.size());
            auto val = reader.move(0);
            for (size_t i = 0; i < calls; ++i, val = reader.next()) {
                do_not_optimize_away(val.second);
            }
            postings += calls;
        }
        double elapsed = get_time_usecs() - tick;
        spdlog::info(
            "Read {} postings in {} seconds, {:.1f} ns per posting",
            postings,
            uint64_t(elapsed / 1000000),
            (elapsed / postings * 1000));
    }

    {
        size_t min_length = 4096;
        spdlog::info("Scanning posting lists longer than {}", min_length);
        std::vector<size_t> long_lists;
        for (size_t i = 0; i < coll.size(); ++i) {
            if (coll[i].size() >= min_length) {
                long_lists.push_back(i);
            }
        }

        auto tick = get_time_usecs();
        uint64_t calls_per_list = 500000;
        size_t postings = 0;
        for (auto i: long_lists) {
            auto reader = coll[i];
            auto calls = std::min(calls_per_list, reader.size());
            auto val = reader.move(0);
            for (size_t i = 0; i < calls; ++i, val = reader.next()) {
                do_not_optimize_away(val.second);
            }
            postings += calls;
        }
        double elapsed = get_time_usecs() - tick;
        spdlog::info(
            "Read {} postings in {} seconds, {:.1f} ns per posting",
            postings,
            uint64_t(elapsed / 1000000),
            (elapsed / postings * 1000));
    }

    uint64_t calls_per_list = 20000;
    for (uint64_t skip = 1; skip <= 16384; skip <<= 1) {
        uint64_t min_length = 1 << 17;
        std::vector<std::pair<size_t, std::vector<uint64_t>>> skip_values;
        std::vector<std::pair<size_t, std::vector<uint64_t>>> skip_positions;
        for (size_t i = 0; i < coll.size(); ++i) {
            auto reader = coll[i];
            if (reader.size() >= min_length) {
                uint64_t size = reader.size();
                // make sure size is odd, so that it is coprime with skip
                if (!(size & 1)) {
                    size -= 1;
                }

                skip_values.emplace_back(i, std::vector<uint64_t>());
                skip_positions.emplace_back(i, std::vector<uint64_t>());
                for (size_t i = 0; i < calls_per_list; ++i) {
                    uint64_t pos = (i * skip) % size;
                    skip_values.back().second.push_back(reader.move(pos).second);
                    skip_positions.back().second.push_back(pos);
                }
            }
        }

        auto tick = get_time_usecs();
        size_t calls = 0;
        for (auto const& p: skip_values) {
            auto reader = coll[p.first];
            for (auto const& val: p.second) {
                do_not_optimize_away(reader.next_geq(val).second);
            }
            calls += p.second.size();
        }
        double elapsed = get_time_usecs() - tick;

        spdlog::info(
            "Performed {} next_geq() with skip={}: {:.1f} ns per call",
            calls,
            skip,
            (elapsed / calls * 1000));

        tick = get_time_usecs();
        calls = 0;
        for (auto const& p: skip_positions) {
            auto reader = coll[p.first];
            for (auto const& pos: p.second) {
                do_not_optimize_away(reader.move(pos).second);
            }
            calls += p.second.size();
        }
        elapsed = get_time_usecs() - tick;

        spdlog::info(
            "Performed {} move() with skip={}: {:.1f} ns per call",
            calls,
            skip,
            (elapsed / calls * 1000));
    }
}
int main(int argc, const char** argv)
{
    using pisa::compact_elias_fano;
    using pisa::indexed_sequence;
    using pisa::partitioned_sequence;
    using pisa::uniform_partitioned_sequence;

    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <collection type> <index filename>" << std::endl;
        return 1;
    }

    std::string type = argv[1];
    const char* index_filename = argv[2];

    if (type == "ef") {
        perftest<compact_elias_fano>(index_filename);
    } else if (type == "is") {
        perftest<indexed_sequence>(index_filename);
    } else if (type == "uniform") {
        perftest<uniform_partitioned_sequence<>>(index_filename);
    } else if (type == "part") {
        perftest<partitioned_sequence<>>(index_filename);
    } else {
        spdlog::error("Unknown type {}", type);
    }
}
