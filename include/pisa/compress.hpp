#pragma once

#include <algorithm>
#include <fstream>
#include <iostream>
#include <numeric>
#include <optional>
#include <thread>

#include <boost/algorithm/string/predicate.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "configuration.hpp"
#include "index_types.hpp"
#include "linear_quantizer.hpp"
#include "mappable/mapper.hpp"
#include "util/index_build_utils.hpp"
#include "util/util.hpp"
#include "util/verify_collection.hpp"  // XXX move to index_build_utils
#include "wand_data.hpp"
#include "wand_data_raw.hpp"

namespace pisa {

template <typename Collection>
void dump_index_specific_stats(Collection const&, std::string const&)
{}

void dump_index_specific_stats(pisa::pefuniform_index const& coll, std::string const& type)
{
    pisa::stats_line()("type", type)("log_partition_size", int(coll.params().log_partition_size));
}

void dump_index_specific_stats(pisa::pefopt_index const& coll, std::string const& type)
{
    auto const& conf = pisa::configuration::get();

    uint64_t length_threshold = 4096;
    double long_postings = 0;
    double docs_partitions = 0;
    double freqs_partitions = 0;

    for (size_t s = 0; s < coll.size(); ++s) {
        auto const& list = coll[s];
        if (list.size() >= length_threshold) {
            long_postings += list.size();
            docs_partitions += list.docs_enum().num_partitions();
            freqs_partitions += list.freqs_enum().base().num_partitions();
        }
    }

    pisa::stats_line()("type", type)("docs_avg_part", long_postings / docs_partitions)(
        "freqs_avg_part", long_postings / freqs_partitions);
}

template <typename CollectionType, typename WandType>
void compress_index(
    binary_freq_collection const& input,
    pisa::global_parameters const& params,
    const std::optional<std::string>& output_filename,
    bool check,
    std::string const& seq_type,
    std::optional<std::string> const& wand_data_filename,
    std::optional<ScorerParam> const& scorer_param,
    bool quantized)
{
    using namespace pisa;
    spdlog::info("Processing {} documents", input.num_docs());
    double tick = get_time_usecs();

    typename CollectionType::builder builder(input.num_docs(), params);
    size_t postings = 0;
    {
        pisa::progress progress("Create index", input.size());
        WandType wdata;
        mio::mmap_source md;
        if (wand_data_filename) {
            std::error_code error;
            md.map(*wand_data_filename, error);
            if (error) {
                spdlog::error("error mapping file: {}, exiting...", error.message());
                std::abort();
            }
            mapper::map(wdata, md, mapper::map_flags::warmup);
        }

        std::unique_ptr<index_scorer<WandType>> scorer;

        if (scorer_param) {
            scorer = scorer::from_param(*scorer_param, wdata);
        }

        size_t term_id = 0;
        for (auto const& plist: input) {
            size_t size = plist.docs.size();
            if (quantized) {
                LinearQuantizer quantizer(
                    wdata.index_max_term_weight(), configuration::get().quantization_bits);
                auto term_scorer = scorer->term_scorer(term_id);
                std::vector<uint64_t> quants;
                for (size_t pos = 0; pos < size; ++pos) {
                    uint64_t doc = *(plist.docs.begin() + pos);
                    uint64_t freq = *(plist.freqs.begin() + pos);
                    float score = term_scorer(doc, freq);
                    uint64_t quant_score = quantizer(score);
                    quants.push_back(quant_score);
                }
                assert(quants.size() == size);
                uint64_t quants_sum =
                    std::accumulate(quants.begin(), quants.begin() + quants.size(), uint64_t(0));
                builder.add_posting_list(size, plist.docs.begin(), quants.begin(), quants_sum);
            } else {
                uint64_t freqs_sum =
                    std::accumulate(plist.freqs.begin(), plist.freqs.begin() + size, uint64_t(0));
                builder.add_posting_list(size, plist.docs.begin(), plist.freqs.begin(), freqs_sum);
            }

            progress.update(1);
            postings += size;
            term_id += 1;
        }
    }

    CollectionType coll;
    builder.build(coll);
    double elapsed_secs = (get_time_usecs() - tick) / 1000000;
    spdlog::info("{} collection built in {} seconds", seq_type, elapsed_secs);

    stats_line()("type", seq_type)("worker_threads", std::thread::hardware_concurrency())(
        "construction_time", elapsed_secs);

    dump_stats(coll, seq_type, postings);
    dump_index_specific_stats(coll, seq_type);

    if (output_filename) {
        mapper::freeze(coll, (*output_filename).c_str());
        if (check and quantized) {
            spdlog::warn("Index construction cannot be verified for quantized indexes.");
        }
        if (check and not quantized) {
            verify_collection<binary_freq_collection, CollectionType>(
                input, (*output_filename).c_str());
        }
    }
}

void compress(
    std::string const& input_basename,
    std::optional<std::string> const& wand_data_filename,
    std::string const& index_encoding,
    std::string const& output_filename,
    std::optional<ScorerParam> const& scorer_param,
    bool quantize,
    bool check)
{
    binary_freq_collection input(input_basename.c_str());
    global_parameters params;

    if (false) {
#define LOOP_BODY(R, DATA, T)                                                    \
    }                                                                            \
    else if (index_encoding == BOOST_PP_STRINGIZE(T))                            \
    {                                                                            \
        compress_index<pisa::BOOST_PP_CAT(T, _index), wand_data<wand_data_raw>>( \
            input,                                                               \
            params,                                                              \
            output_filename,                                                     \
            check,                                                               \
            index_encoding,                                                      \
            wand_data_filename,                                                  \
            scorer_name,                                                         \
            quantize);                                                           \
        /**/
        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY
    } else {
        spdlog::error("Unknown type {}", index_encoding);
        std::abort();
    }
}

}  // namespace pisa
