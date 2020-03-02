#include <iostream>
#include <optional>

#include <CLI/CLI.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <mio/mmap.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "index_types.hpp"
#include "io.hpp"
#include "mappable/mapper.hpp"
#include "query/algorithm.hpp"
#include "scorer/scorer.hpp"
#include "util/util.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"


using namespace pisa;

template <typename IndexType, typename WandType>
void thresholds(const std::string &index_filename,
                const std::optional<std::string> &wand_data_filename,
                const std::vector<Query> &queries,
                std::string const &type,
                std::string const &scorer_name,
                uint64_t k,
                bool quantized)
{
    IndexType index;
    mio::mmap_source m(index_filename.c_str());
    mapper::map(index, m);

    WandType wdata;

    auto scorer = scorer::from_name(scorer_name, wdata);

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
    topk_queue topk(k);
    wand_query wand_q(topk);
    for (auto const &query : queries) {
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

int main(int argc, const char **argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    // set full precision for floats
    std::cout.precision(std::numeric_limits<float>::max_digits10);

    bool quantized = false;

    App<arg::Index, arg::WandData, arg::Query<arg::QueryMode::Ranked>, arg::Scorer> app{
        "Extracts query thresholds."};
    app.add_flag("--quantized", quantized, "Quantizes the scores");

    CLI11_PARSE(app, argc, argv);

    /**/
    if (false) {
#define LOOP_BODY(R, DATA, T)                                                                 \
    }                                                                                         \
    else if (app.index_encoding() == BOOST_PP_STRINGIZE(T))                                   \
    {                                                                                         \
        if (app.is_wand_compressed()) {                                                       \
            if (quantized) {                                                                  \
                thresholds<BOOST_PP_CAT(T, _index), wand_uniform_index_quantized>(            \
                    app.index_filename(),                                                     \
                    app.wand_data_path(),                                                     \
                    app.queries(),                                                            \
                    app.index_encoding(),                                                     \
                    app.scorer(),                                                             \
                    app.k(),                                                                  \
                    quantized);                                                               \
            } else {                                                                          \
                thresholds<BOOST_PP_CAT(T, _index), wand_uniform_index>(app.index_filename(), \
                                                                        app.wand_data_path(), \
                                                                        app.queries(),        \
                                                                        app.index_encoding(), \
                                                                        app.scorer(),         \
                                                                        app.k(),              \
                                                                        quantized);           \
            }                                                                                 \
        } else {                                                                              \
            thresholds<BOOST_PP_CAT(T, _index), wand_raw_index>(app.index_filename(),         \
                                                                app.wand_data_path(),         \
                                                                app.queries(),                \
                                                                app.index_encoding(),         \
                                                                app.scorer(),                 \
                                                                app.k(),                      \
                                                                quantized);                   \
        }                                                                                     \
        /**/

        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY

    } else {
        spdlog::error("Unknown type {}", app.index_encoding());
    }
}
