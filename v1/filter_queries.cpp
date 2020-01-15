#include <iostream>

#include <CLI/CLI.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "app.hpp"

namespace arg = pisa::arg;

int main(int argc, char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    std::size_t min_query_len = 1;

    pisa::App<arg::Index, arg::Query<arg::QueryMode::Unranked>> app(
        "Filters out empty queries against a v1 index.");
    app.add_option("--min", min_query_len, "Minimum query legth to consider");
    CLI11_PARSE(app, argc, argv);

    auto meta = app.index_metadata();
    for (auto&& query : app.query_range(meta)) {
        if (query.get_term_ids().size() >= min_query_len) {
            std::cout << *query.to_json() << '\n';
        }
    }
    return 0;
}
