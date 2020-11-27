#include <algorithm>
#include <iostream>
#include <numeric>
#include <optional>
#include <string>

#include <CLI/CLI.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <mio/mmap.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/sinks/null_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "accumulator/lazy_accumulator.hpp"
#include "app.hpp"
#include "binary_index.hpp"
#include "cursor/block_max_scored_cursor.hpp"
#include "cursor/cursor.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/scored_cursor.hpp"
#include "index_types.hpp"
#include "mappable/mapper.hpp"
#include "memory_source.hpp"
#include "query/algorithm.hpp"
#include "scorer/scorer.hpp"
#include "timer.hpp"
#include "topk_queue.hpp"
#include "util/util.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

using namespace pisa;

template <typename IndexType, typename WandType>
void selections(
    std::string const& index_filename,
    std::string const& wand_data_filename,
    std::vector<QueryContainer> const& queries,
    std::uint64_t k,
    std::string const& pair_index_path,
    float pair_cost_scaling,
    SelectionMethod method)
{
    auto const index = IndexType(MemorySource::mapped_file(index_filename));
    auto const wdata = WandType(MemorySource::mapped_file(wand_data_filename));

    using pair_index_type = PairIndex<block_freq_index<simdbp_block, false, IndexArity::Binary>>;
    auto pair_index = pair_index_type::load(pair_index_path, true);

    for (auto const& query: queries) {
        try {
            auto threshold = query.threshold(k);
            auto request = query.query(k);
            auto timed_result = run_with_timer_ret<std::chrono::microseconds>([&]() {
                auto lattice = pisa::IntersectionLattice<std::uint16_t>::build(
                    request, index, wdata, pair_index, pair_cost_scaling);
                return select_intersections(request, lattice, *threshold, method);
            });
            std::vector<std::array<TermId, 2>> pairs;
            std::transform(
                timed_result.result.selection.selected_pairs.begin(),
                timed_result.result.selection.selected_pairs.end(),
                std::back_inserter(pairs),
                [](TermPair term_pair) -> std::array<TermId, 2> {
                    return {term_pair.get<0>(), term_pair.get<1>()};
                });
            nlohmann::json sel;
            sel["single"] = timed_result.result.selection.selected_terms;
            sel["pairs"] = pairs;
            sel["cost"] = timed_result.result.cost;
            sel["time"] = timed_result.time.count();
            sel["method"] = method == SelectionMethod::Greedy ? "greedy" : "brute-force";
            std::cout << sel << '\n';
        } catch (std::invalid_argument const& err) {
            throw std::runtime_error(fmt::format(
                "Error while executing query: {}\n{}", err.what(), query.to_json_string(2)));
        }
    }
}

using wand_raw_index = wand_data<wand_data_raw>;
using wand_uniform_index = wand_data<wand_data_compressed<>>;
using wand_uniform_index_quantized = wand_data<wand_data_compressed<PayloadType::Quantized>>;

int main(int argc, const char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    bool silent = false;
    bool safe = false;
    bool quantized = false;
    bool use_thresholds = false;
    bool disk_resident = false;
    bool disk_resident_pairs = false;
    bool brute_force = false;
    std::string pair_index_path;
    float pair_cost_scaling = 1.0;

    App<arg::Index, arg::WandData<arg::WandMode::Required>, arg::Query<arg::QueryMode::Ranked>> app{
        "Intersection selection."};
    app.add_flag("--quantized", quantized, "Quantized scores");
    app.add_flag("--silent", silent, "Suppress logging");
    app.add_option("--pair-index", pair_index_path, "Path to pair index.")->required();
    app.add_flag(
        "--disk-resident", disk_resident, "Keep index on disk and load postings at query time.");
    app.add_flag(
        "--disk-resident-pairs",
        disk_resident_pairs,
        "Keep pair index on disk and load postings at query time.");
    app.add_option(
        "--scale",
        pair_cost_scaling,
        "Scaling factor for pair intersection costs when selecting essential posting lists with "
        "intersections.");
    app.add_flag(
        "--brute-force", brute_force, "Use brute force method (extremely slow for longer queries).");
    CLI11_PARSE(app, argc, argv);

    if (silent) {
        spdlog::set_default_logger(spdlog::create<spdlog::sinks::null_sink_mt>("stderr"));
    } else {
        spdlog::set_default_logger(spdlog::stderr_color_mt("stderr"));
    }

    std::vector<pisa::QueryContainer> queries;
    try {
        auto reader = app.resolved_query_reader();
        reader.for_each([&](auto&& query) { queries.push_back(query); });
    } catch (pisa::MissingResolverError err) {
        spdlog::error("Unresoved queries (without IDs) require term lexicon.");
        std::exit(1);
    } catch (std::runtime_error const& err) {
        spdlog::error(err.what());
        std::exit(1);
    }

    auto params = std::make_tuple(
        app.index_filename(),
        app.wand_data_path(),
        queries,
        app.k(),
        pair_index_path,
        pair_cost_scaling,
        brute_force ? SelectionMethod::BruteForce : SelectionMethod::Greedy);
    /**/
    if (false) {
#define LOOP_BODY(R, DATA, T)                                                                   \
    }                                                                                           \
    else if (app.index_encoding() == BOOST_PP_STRINGIZE(T))                                     \
    {                                                                                           \
        if (app.is_wand_compressed()) {                                                         \
            if (quantized) {                                                                    \
                std::apply(                                                                     \
                    selections<BOOST_PP_CAT(T, _index), wand_uniform_index_quantized>, params); \
            } else {                                                                            \
                std::apply(selections<BOOST_PP_CAT(T, _index), wand_uniform_index>, params);    \
            }                                                                                   \
        } else {                                                                                \
            std::apply(selections<BOOST_PP_CAT(T, _index), wand_raw_index>, params);            \
        }
        /**/
        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY

    } else {
        spdlog::error("Unknown type {}", app.index_encoding());
    }
}
