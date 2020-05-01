#include <iostream>

#include <CLI/CLI.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "query.hpp"
#include "query/query_parser.hpp"
#include "query/term_resolver.hpp"
#include "tokenizer.hpp"

namespace arg = pisa::arg;

using pisa::filter_queries;
using pisa::QueryContainer;
using pisa::QueryParser;
using pisa::QueryReader;
using pisa::StandardTermResolver;
using pisa::TermResolver;
using pisa::io::for_each_line;

int main(int argc, char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    std::size_t min_query_len = 1;
    std::size_t max_query_len = std::numeric_limits<std::size_t>::max();

    pisa::App<arg::Query<arg::QueryMode::Unranked>> app("Filters queries by their length");
    app.add_option("--min", min_query_len, "Minimum query legth to consider");
    app.add_option("--max", max_query_len, "Maximum query legth to consider");
    CLI11_PARSE(app, argc, argv);

    std::optional<StandardTermResolver> term_resolver{};
    if (app.term_lexicon()) {
        term_resolver = StandardTermResolver(*app.term_lexicon(), app.stop_words(), app.stemmer());
    }

    try {
        filter_queries(
            app.query_file(), std::move(term_resolver), min_query_len, max_query_len, std::cout);
        return 0;
    } catch (pisa::MissingResolverError err) {
        spdlog::error("Unresoved queries(without IDs) require term lexicon.");
    } catch (std::runtime_error const& err) {
        spdlog::error(err.what());
        return 1;
    }
}
