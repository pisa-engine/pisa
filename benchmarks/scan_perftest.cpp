#include "mio/mmap.hpp"
#include "succinct/mapper.hpp"
#include "sequence_collection.hpp"
#include "sequence/partitioned_sequence.hpp"
#include "sequence/uniform_partitioned_sequence.hpp"
#include "util/util.hpp"

using ds2i::logger;
using ds2i::get_time_usecs;
using ds2i::do_not_optimize_away;

template <typename BaseSequence>
void perftest(const char* index_filename)
{
    typedef ds2i::sequence_collection<BaseSequence> collection_type;
    logger() << "Loading collection from " << index_filename << std::endl;
    collection_type coll;
    mio::mmap_source m(index_filename);
    ds2i::mapper::map(coll, m, ds2i::mapper::map_flags::warmup);

    if (true) {
        logger() << "Scanning all the posting lists" << std::endl;
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
        logger() << "Read " << postings << " postings in "
                 << uint64_t(elapsed / 1000000) << " seconds, "
                 << std::fixed << std::setprecision(1)
                 << (elapsed / postings * 1000) << " ns per posting"
                 << std::endl;
    }

    {
        size_t min_length = 4096;
        logger() << "Scanning posting lists longer than " << min_length << std::endl;
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
        logger() << "Read " << postings << " postings in "
                 << uint64_t(elapsed / 1000000) << " seconds, "
                 << std::fixed << std::setprecision(1)
                 << (elapsed / postings * 1000) << " ns per posting"
                 << std::endl;
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
                if (!(size & 1)) size -= 1;

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

        logger() << "Performed " << calls << " next_geq() with skip=" << skip <<": "
                 << std::fixed << std::setprecision(1)
                 << (elapsed / calls * 1000) << " ns per call"
                 << std::endl;

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

        logger() << "Performed " << calls << " move() with skip=" << skip <<": "
                 << std::fixed << std::setprecision(1)
                 << (elapsed / calls * 1000) << " ns per call"
                 << std::endl;
    }
}
int main(int argc, const char** argv) {

    using ds2i::compact_elias_fano;
    using ds2i::indexed_sequence;
    using ds2i::partitioned_sequence;
    using ds2i::uniform_partitioned_sequence;

    if (argc != 3) {
        std::cerr << "Usage: " << argv[0]
                  << " <collection type> <index filename>"
                  << std::endl;
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
        logger() << "ERROR: Unknown type " << type << std::endl;
    }
}
