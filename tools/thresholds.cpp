#include <iostream>
#include <optional>
#include <tuple>

#include <CLI/CLI.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <mio/mmap.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/scored_cursor.hpp"
#include "index_types.hpp"
#include "io.hpp"
#include "mappable/mapper.hpp"
#include "memory_source.hpp"
#include "query/algorithm.hpp"
#include "scorer/scorer.hpp"
#include "util/util.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

using namespace pisa;

template <typename Index, typename Wdata>
void thresholds(
    Index&& index,
    Wdata&& wdata,
    const std::vector<Query>& queries,
    std::string const& type,
    ScorerParams const& scorer_params,
    uint64_t k,
    bool quantized)
{
    auto scorer = scorer::from_params(scorer_params, wdata);

    topk_queue topk(k);
    wand_query wand_q(topk);
    for (auto const& query: queries) {
        wand_q(make_max_scored_cursors(index, wdata, *scorer, query), index.num_docs());
        topk.finalize();
        auto results = topk.topk();
        topk.clear();
        float threshold = 0.0;
        if (results.size() == k) {
            threshold = results.back().first;
        }
        std::cout << threshold << '\n';
    }
}

using wand_raw_index = wand_data<wand_data_raw>;
using wand_uniform_index = wand_data<wand_data_compressed<>>;
using wand_uniform_index_quantized = wand_data<wand_data_compressed<PayloadType::Quantized>>;

int main(int argc, const char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    // set full precision for floats
    std::cout.precision(std::numeric_limits<float>::max_digits10);

    bool quantized = false;

    App<arg::Index, arg::WandData<arg::WandMode::Required>, arg::Query<arg::QueryMode::Ranked>, arg::Scorer>
        app{"Extracts query thresholds."};
    app.add_flag("--quantized", quantized, "Quantizes the scores");

    CLI11_PARSE(app, argc, argv);

    auto params = std::make_tuple(
        app.index_filename(),
        app.wand_data_path(),
        app.queries(),
        app.index_encoding(),
        app.scorer_params(),
        app.k(),
        quantized);

    try {
        IndexType::resolve(app.index_encoding()).load_and_execute(app.index_filename(), [&](auto&& index) {
            auto th = [&](auto wdata) {
                thresholds(
                    index,
                    wdata,
                    app.queries(),
                    app.index_encoding(),
                    app.scorer_params(),
                    app.k(),
                    quantized);
            };
            auto wdata_source = MemorySource::mapped_file(app.wand_data_path());
            if (app.is_wand_compressed()) {
                if (quantized) {
                    th(wand_uniform_index_quantized(std::move(wdata_source)));
                } else {
                    th(wand_uniform_index(std::move(wdata_source)));
                }
            } else {
                th(wand_raw_index(std::move(wdata_source)));
            }
        });
    } catch (std::exception const& err) {
        spdlog::error("{}", err.what());
        return 1;
    }
    return 0;
}
