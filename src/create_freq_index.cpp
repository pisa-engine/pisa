#include <algorithm>
#include <fstream>
#include <iostream>
#include <numeric>
#include <thread>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/optional.hpp>

#include "succinct/mapper.hpp"

#include "scorer/bm25.hpp"
#include "configuration.hpp"
#include "util/index_build_utils.hpp"
#include "index_types.hpp"
#include "util/util.hpp"
#include "util/verify_collection.hpp" // XXX move to index_build_utils

#include "cxxopts.hpp"

using ds2i::logger;

template <typename Collection>
void dump_index_specific_stats(Collection const &, std::string const &) {}

void dump_index_specific_stats(ds2i::uniform_index const &coll, std::string const &type) {
    ds2i::stats_line()("type", type)("log_partition_size", int(coll.params().log_partition_size));
}

void dump_index_specific_stats(ds2i::opt_index const &coll, std::string const &type) {
    auto const &conf = ds2i::configuration::get();

    uint64_t length_threshold = 4096;
    double long_postings = 0;
    double docs_partitions = 0;
    double freqs_partitions = 0;

    for (size_t s = 0; s < coll.size(); ++s) {
        auto const &list = coll[s];
        if (list.size() >= length_threshold) {
            long_postings += list.size();
            docs_partitions += list.docs_enum().num_partitions();
            freqs_partitions += list.freqs_enum().base().num_partitions();
        }
    }

    ds2i::stats_line()("type", type)("eps1", conf.eps1)("eps2", conf.eps2)(
        "fix_cost", conf.fix_cost)("docs_avg_part", long_postings / docs_partitions)(
        "freqs_avg_part", long_postings / freqs_partitions);
}

template <typename InputCollection, typename CollectionType, typename Scorer = ds2i::bm25>
void create_collection(InputCollection const &input,
                       ds2i::global_parameters const &params,
                       boost::optional<std::string> &output_filename,
                       bool check,
                       std::string const &seq_type) {
    using namespace ds2i;
    logger() << "Processing " << input.num_docs() << " documents" << std::endl;
    double tick = get_time_usecs();

    typename CollectionType::builder builder(input.num_docs(), params);
    progress_logger plog;
    uint64_t size = 0;

    for (auto const &plist : input) {
        uint64_t freqs_sum;
        size = plist.docs.size();
        freqs_sum = std::accumulate(plist.freqs.begin(), plist.freqs.begin() + size, uint64_t(0));
        builder.add_posting_list(size, plist.docs.begin(), plist.freqs.begin(), freqs_sum);

        plog.done_sequence(size);
    }

    plog.log();
    CollectionType coll;
    builder.build(coll);
    double elapsed_secs = (get_time_usecs() - tick) / 1000000;
    logger() << seq_type << " collection built in " << elapsed_secs << " seconds" << std::endl;

    stats_line()("type", seq_type)("worker_threads", configuration::get().worker_threads)(
        "construction_time", elapsed_secs);

    dump_stats(coll, seq_type, plog.postings);
    dump_index_specific_stats(coll, seq_type);

    if (output_filename) {
        mapper::freeze(coll, output_filename.value().c_str());
        if (check) {
            verify_collection<InputCollection, CollectionType>(input,
                                                               output_filename.value().c_str());
        }
    }
}

int main(int argc, char **argv) {

    using namespace ds2i;
    std::string type;
    std::string input_basename;
    boost::optional<std::string> output_filename;
    bool check = false;

    cxxopts::Options options("create_freq_index",
                             "create_freq_index - a tool for creating an index.");
    options.add_options()
        ("h,help", "Print help")
        ("t,type", "Index type", cxxopts::value(type), "type_name")
        ("c,collection", "Collection basename", cxxopts::value(input_basename), "basename")
        ("o,out", "Output filename", cxxopts::value<std::string>(), "filename")
        ("check", "Check the correctness of the index", cxxopts::value(check));

    try {
        options.parse(argc, argv);
        if (options.count("help") == 1) {
            std::cout << options.help(options.groups()) << std::endl;
            exit(1);
        }
        if (options.count("out") == 1) {
            output_filename = options["out"].as<std::string>();
        }
        cxxopts::check_required(options, {"type"});
        cxxopts::check_required(options, {"collection"});
    } catch (const cxxopts::OptionException &e) {
        std::cout << "ERROR: " << e.what() << "\n";
        std::cout << options.help(options.groups()) << std::endl;
        exit(1);
    }
    binary_freq_collection input(input_basename.c_str());

    ds2i::global_parameters params;
    params.log_partition_size = configuration::get().log_partition_size;

    if (false) {
#define LOOP_BODY(R, DATA, T)                                               \
    }                                                                       \
    else if (type == BOOST_PP_STRINGIZE(T)) {                               \
        create_collection<binary_freq_collection, BOOST_PP_CAT(T, _index)>( \
            input, params, output_filename, check, type);                   \
        /**/

        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, DS2I_INDEX_TYPES);
#undef LOOP_BODY
    } else {
        logger() << "ERROR: Unknown type " << type << std::endl;
    }

    return 0;
}
