#include <iostream>
#include <tuple>

#include <CLI/CLI.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <mio/mmap.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "index_types.hpp"
#include "memory_source.hpp"
#include "query/algorithm/wand_query.hpp"
#include "scorer/scorer.hpp"
#include "wand_data.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

using namespace pisa;

template <typename IndexType, typename WandType>
void thresholds(
    IndexType const* index_ptr,
    const std::string& wand_data_filename,
    const std::vector<Query>& queries,
    std::string const& type,
    ScorerParams const& scorer_params,
    uint64_t k,
    bool quantized
) {
    auto const& index = *index_ptr;
    WandType const wdata(MemorySource::mapped_file(wand_data_filename));

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

int main(int argc, const char** argv) {
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    // set full precision for floats
    std::cout.precision(std::numeric_limits<float>::max_digits10);

    bool quantized = false;

    App<arg::Index, arg::WandData<arg::WandMode::Required>, arg::Query<arg::QueryMode::Ranked>, arg::Scorer, arg::LogLevel>
        app{"Extracts query thresholds."};
    app.add_flag("--quantized", quantized, "Quantizes the scores");

    CLI11_PARSE(app, argc, argv);
    spdlog::set_level(app.log_level());

    run_for_index(app.index_encoding(), MemorySource::mapped_file(app.index_filename()), [&](auto index) {
        using Index = std::decay_t<decltype(index)>;
        auto params = std::make_tuple(
            &index,
            app.wand_data_path(),
            app.queries(),
            app.index_encoding(),
            app.scorer_params(),
            app.k(),
            quantized
        );
        if (app.is_wand_compressed()) {
            if (quantized) {
                std::apply(thresholds<Index, wand_uniform_index_quantized>, params);
            } else {
                std::apply(thresholds<Index, wand_uniform_index>, params);
            }
        } else {
            std::apply(thresholds<Index, wand_raw_index>, params);
        }
    });
}
