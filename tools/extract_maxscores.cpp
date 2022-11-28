#include <algorithm>
#include <iostream>
#include <numeric>
#include <vector>

#include <CLI/CLI.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "wand_data.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

using namespace pisa;

template <typename Wand>
void extract(
    std::string const& wand_data_path,
    std::vector<pisa::Query> const& queries,
    std::string const& separator,
    bool print_query_id)
{
    Wand wdata(MemorySource::mapped_file(wand_data_path));
    for (auto const& query: queries) {
        if (print_query_id and query.id) {
            std::cout << *(query.id) << ":";
        }
        std::cout << boost::algorithm::join(
            query.terms | boost::adaptors::transformed([&wdata](auto term_id) {
                return std::to_string(wdata.max_term_weight(term_id));
            }),
            separator);
        std::cout << '\n';
    }
}

using wand_raw_index = wand_data<wand_data_raw>;
using wand_uniform_index = wand_data<wand_data_compressed<>>;
using wand_uniform_index_quantized = wand_data<wand_data_compressed<PayloadType::Quantized>>;

int main(int argc, char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    bool quantized = false;

    App<arg::WandData<arg::WandMode::Required>,
        arg::Query<arg::QueryMode::Unranked>,
        arg::Separator,
        arg::PrintQueryId,
        arg::LogLevel>
        app{
            R"(
Extracts max-scores for query terms from an inverted index.

The max-scores will be printed to the output separated by --sep,
which is a tab by default.)"};
    app.add_flag("--quantized", quantized, "Quantized scores");
    CLI11_PARSE(app, argc, argv);

    spdlog::set_level(app.log_level());

    auto params =
        std::make_tuple(app.wand_data_path(), app.queries(), app.separator(), app.print_query_id());

    if (app.is_wand_compressed()) {
        if (quantized) {
            std::apply(extract<wand_uniform_index_quantized>, params);
        } else {
            std::apply(extract<wand_uniform_index>, params);
        }
    } else {
        std::apply(extract<wand_raw_index>, params);
    }

    return 0;
}
