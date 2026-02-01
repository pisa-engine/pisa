#include <optional>
#include <string>

#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "block_inverted_index.hpp"
#include "codec/block_codec.hpp"
#include "codec/block_codec_registry.hpp"
#include "compress.hpp"
#include "index_types.hpp"
#include "linear_quantizer.hpp"
#include "type_safe.hpp"
#include "util/index_build_utils.hpp"
#include "util/progress.hpp"
#include "util/verify_collection.hpp"
#include "wand_data.hpp"
#include "wand_data_raw.hpp"

namespace pisa {

template <typename Collection>
void dump_index_specific_stats(Collection const&, std::string const&) {}

void dump_index_specific_stats(pisa::pefuniform_index const& coll, std::string const& type) {
    std::cout << pisa::stats_builder()
                     .add("type", type)
                     .add("log_partition_size", int(coll.params().log_partition_size))
                     .build();
}

void dump_index_specific_stats(pisa::pefopt_index const& coll, std::string const& type) {
    std::uint64_t length_threshold = 4096;
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

    std::cout << pisa::stats_builder()
                     .add("type", type)
                     .add("docs_avg_part", long_postings / docs_partitions)
                     .add("freqs_avg_part", long_postings / freqs_partitions)
                     .build();
}

template <typename CollectionType, typename Wand>
void compress_index_streaming(
    binary_freq_collection const& input,
    pisa::global_parameters const& params,
    std::string const& output_filename,
    std::optional<QuantizingScorer> quantizing_scorer,
    bool check
) {
    spdlog::info("Processing {} documents (streaming)", input.num_docs());
    double tick = get_time_usecs();

    typename CollectionType::stream_builder builder(input.num_docs(), params);
    {
        pisa::progress progress("Create index", input.size());

        size_t term_id = 0;
        if (quantizing_scorer) {
            std::vector<std::uint64_t> quantized_scores;
            for (auto const& plist: input) {
                auto term_scorer = quantizing_scorer->term_scorer(term_id);
                std::size_t size = plist.docs.size();
                for (size_t pos = 0; pos < size; ++pos) {
                    auto doc = *(plist.docs.begin() + pos);
                    auto freq = *(plist.freqs.begin() + pos);
                    auto score = term_scorer(doc, freq);
                    quantized_scores.push_back(score);
                }
                auto sum = std::accumulate(
                    quantized_scores.begin(), quantized_scores.end(), std::uint64_t(0)
                );
                builder.add_posting_list(size, plist.docs.begin(), quantized_scores.begin(), sum);
                term_id += 1;
                quantized_scores.clear();
                progress.update(1);
            }
        } else {
            for (auto const& plist: input) {
                size_t size = plist.docs.size();
                uint64_t freqs_sum =
                    std::accumulate(plist.freqs.begin(), plist.freqs.begin() + size, uint64_t(0));
                builder.add_posting_list(size, plist.docs.begin(), plist.freqs.begin(), freqs_sum);
                progress.update(1);
                term_id += 1;
            }
        }
    }

    builder.build(output_filename);
    double elapsed_secs = (get_time_usecs() - tick) / 1000000;
    spdlog::info("Index compressed in {} seconds", elapsed_secs);

    if (check) {
        verify_collection<binary_freq_collection, CollectionType>(
            input, output_filename.c_str(), std::move(quantizing_scorer)
        );
    }
}

// TODO(michal): Group parameters under a common `optional` so that, say, it is impossible to get
// `quantized == true` and at the same time `wand_data_filename == std::nullopt`.
template <typename CollectionType, typename WandType>
void compress_index(
    binary_freq_collection const& input,
    pisa::global_parameters const& params,
    const std::optional<std::string>& output_filename,
    bool check,
    std::string const& seq_type,
    std::optional<std::string> const& wand_data_filename,
    ScorerParams const& scorer_params,
    std::optional<Size> quantization_bits
) {
    std::optional<QuantizingScorer> quantizing_scorer{};

    spdlog::info("Processing {} documents", input.num_docs());
    double tick = get_time_usecs();

    WandType const wdata = [&] {
        if (wand_data_filename) {
            return WandType(MemorySource::mapped_file(*wand_data_filename));
        }
        return WandType{};
    }();

    if (quantization_bits.has_value()) {
        std::unique_ptr<IndexScorer> scorer = scorer::from_params(scorer_params, wdata);
        LinearQuantizer quantizer(wdata.index_max_term_weight(), quantization_bits->as_int());
        quantizing_scorer.emplace(std::move(scorer), quantizer);
    }

    typename CollectionType::builder builder(input.num_docs(), params);
    size_t postings = 0;
    {
        pisa::progress progress("Create index", input.size());

        size_t term_id = 0;
        for (auto const& plist: input) {
            size_t size = plist.docs.size();
            if (quantizing_scorer.has_value()) {
                auto term_scorer = quantizing_scorer->term_scorer(term_id);
                std::vector<uint64_t> quants;
                for (size_t pos = 0; pos < size; ++pos) {
                    uint64_t doc = *(plist.docs.begin() + pos);
                    uint64_t freq = *(plist.freqs.begin() + pos);
                    uint64_t quant_score = term_scorer(doc, freq);
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

    std::cout << pisa::stats_builder()
                     .add("type", seq_type)
                     .add("worker_threads", std::thread::hardware_concurrency())
                     .add("construction_time", elapsed_secs)
                     .build();

    dump_stats(coll, seq_type, postings);
    dump_index_specific_stats(coll, seq_type);

    if (output_filename) {
        mapper::freeze(coll, (*output_filename).c_str());
        if (check) {
            verify_collection<binary_freq_collection, CollectionType>(
                input, (*output_filename).c_str(), std::move(quantizing_scorer)
            );
        }
    }
}

void compress(
    std::string const& input_basename,
    std::optional<std::string> const& wand_data_filename,
    std::string const& index_encoding,
    std::string const& output_filename,
    ScorerParams const& scorer_params,
    std::optional<Size> quantization_bits,
    bool check,
    bool in_memory
) {
    binary_freq_collection input(input_basename.c_str());
    global_parameters params;

    auto block_codec = get_block_codec(index_encoding);
    if (block_codec != nullptr) {
        BlockIndexBuilder builder(std::move(block_codec), scorer_params);
        builder.check(check).in_memory(in_memory);
        std::optional<wand_data<wand_data_raw>> wdata{};
        if (quantization_bits.has_value()) {
            wdata.emplace(MemorySource::mapped_file(*wand_data_filename));
            builder.quantize(*quantization_bits, *wdata);
        }
        builder.build(input, output_filename);
        return;
    }

    resolve_freq_index_type(index_encoding, [&](auto index_traits) {
        using Index = typename std::decay_t<decltype(index_traits)>::type;
        compress_index<Index, wand_data<wand_data_raw>>(
            input, params, output_filename, check, index_encoding, wand_data_filename, scorer_params, quantization_bits
        );
    });
}

}  // namespace pisa
