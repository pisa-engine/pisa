#include <algorithm>
#include <fstream>
#include <iostream>
#include <numeric>
#include <thread>
#include <optional>

#include "boost/algorithm/string/predicate.hpp"
#include "spdlog/spdlog.h"

#include "mappable/mapper.hpp"

#include "scorer/bm25.hpp"
#include "configuration.hpp"
#include "util/index_build_utils.hpp"
#include "index_types.hpp"
#include "util/util.hpp"
#include "util/verify_collection.hpp" // XXX move to index_build_utils

#include "CLI/CLI.hpp"

template <typename Collection>
void dump_index_specific_stats(Collection const &, std::string const &) {}

void dump_index_specific_stats(pisa::uniform_index const &coll, std::string const &type) {
    pisa::stats_line()("type", type)("log_partition_size", int(coll.params().log_partition_size));
}

void dump_index_specific_stats(pisa::opt_index const &coll, std::string const &type) {
    auto const &conf = pisa::configuration::get();

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

    pisa::stats_line()("type", type)("eps1", conf.eps1)("eps2", conf.eps2)(
        "fix_cost", conf.fix_cost)("docs_avg_part", long_postings / docs_partitions)(
        "freqs_avg_part", long_postings / freqs_partitions);
}

template <typename InputCollection, typename Index, typename Builder, typename Scorer = pisa::bm25>
void create_collection(InputCollection const &input,
                       std::unique_ptr<Builder> builder_ptr,
                       std::unique_ptr<Index> index,
                       const std::optional<std::string> &output_filename,
                       bool check,
                       std::string const &seq_type)
{
    auto &builder = *builder_ptr;
    using namespace pisa;
    spdlog::info("Processing {} documents", input.num_docs());
    double tick = get_time_usecs();

    uint64_t size = 0;
    size_t postings = 0;
    {
        pisa::progress progress("Create index", input.size());
        for (auto const &plist : input) {
            uint64_t freqs_sum;
            size = plist.docs.size();
            freqs_sum = std::accumulate(plist.freqs.begin(), plist.freqs.begin() + size, uint64_t(0));
            builder.add_posting_list(size, plist.docs.begin(), plist.freqs.begin(), freqs_sum);

            progress.update(1);
            postings += size;
        }
    }

    auto &coll = *index;
    builder.build(coll);
    double elapsed_secs = (get_time_usecs() - tick) / 1000000;
    spdlog::info("{} collection built in {} seconds", seq_type, elapsed_secs);

    stats_line()("type", seq_type)("worker_threads", configuration::get().worker_threads)(
        "construction_time", elapsed_secs);

    dump_stats(coll, seq_type, postings);
    dump_index_specific_stats(coll, seq_type);

    if (output_filename) {
        mapper::freeze(coll, (*output_filename).c_str());
        if (check) {
            if constexpr (std::is_same_v<Index, block_freq_index<>>) {
                verify_collection<InputCollection, Index>(
                    input, (*output_filename).c_str(), block_freq_index<>::from_type(seq_type));
            } else {
                verify_collection<InputCollection, Index>(
                    input, (*output_filename).c_str(), std::make_unique<Index>());
            }
        }
    }
}

int main(int argc, char **argv) {

    using namespace pisa;
    std::string type;
    std::string input_basename;
    std::optional<std::string> output_filename;
    bool check = false;

    CLI::App app{"create_freq_index - a tool for creating an index."};
    app.add_option("-t,--type", type, "Index type")->required();
    app.add_option("-c,--collection", input_basename, "Collection basename")->required();
    app.add_option("-o,--output", output_filename, "Output filename")->required();
    app.add_flag("--check", check, "Check the correctness of the index");
    CLI11_PARSE(app, argc, argv);

    binary_freq_collection input(input_basename.c_str());

    pisa::global_parameters params;
    params.log_partition_size = configuration::get().log_partition_size;

    if (false) {
#define LOOP_BODY(R, DATA, T)                                                                      \
    }                                                                                              \
    else if (type == BOOST_PP_STRINGIZE(T))                                                        \
    {                                                                                              \
        create_collection<binary_freq_collection, BOOST_PP_CAT(T, _index)>(                        \
            input,                                                                                 \
            std::make_unique<typename BOOST_PP_CAT(T, _index)::builder>(input.num_docs(), params), \
            std::make_unique<BOOST_PP_CAT(T, _index)>(),                                           \
            output_filename,                                                                       \
            check,                                                                                 \
            type);                                                                                 \
        /**/

        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY
    } else if (auto codec = BlockCodec::from_type(type); codec != std::nullopt) {
        create_collection<binary_freq_collection, block_freq_index<>>(
            input,
            std::make_unique<typename block_freq_index<>::builder>(
                input.num_docs(), params, *codec),
            std::make_unique<block_freq_index<>>(*codec),
            output_filename,
            check,
            type);
    } else {
        spdlog::error("Unknown type {}", type);
    }

    return 0;
}
