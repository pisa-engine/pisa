#include <iostream>

#include <CLI/CLI.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "query.hpp"
#include "query/parser.hpp"
#include "tokenizer.hpp"

namespace arg = pisa::arg;

using pisa::QueryContainer;
using pisa::QueryParser;
using pisa::QueryReader;
using pisa::StandardTermResolver;
using pisa::io::for_each_line;

enum class Format { Json, Colon };

void filter_queries(
    std::optional<std::string> const& query_file,
    std::optional<std::string> const& term_lexicon,
    std::optional<std::string> const& stemmer,
    std::optional<std::string> const& stopwords_filename,
    std::size_t min_query_len,
    std::size_t max_query_len)
{
    auto reader = [&] {
        if (query_file) {
            return QueryReader::from_file(*query_file);
        }
        return QueryReader::from_stdin();
    }();
    reader.for_each([&](auto query) {
        if (not query.term_ids()) {
            if (not term_lexicon) {
                throw std::runtime_error("Unresoved queries (without IDs) require term lexicon.");
            }
            query.parse(QueryParser(StandardTermResolver(*term_lexicon, stopwords_filename, stemmer)));
        }
        if (auto len = query.term_ids()->size(); len >= min_query_len && len <= max_query_len) {
            std::cout << query.to_json() << '\n';
        }
    });
}

int main(int argc, char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    std::size_t min_query_len = 1;
    std::size_t max_query_len = std::numeric_limits<std::size_t>::max();

    pisa::App<arg::Query<arg::QueryMode::Unranked>> app(
        "Filters out empty queries against a v1 index.");
    app.add_option("--min", min_query_len, "Minimum query legth to consider");
    app.add_option("--max", max_query_len, "Maximum query legth to consider");
    CLI11_PARSE(app, argc, argv);

    try {
        filter_queries(
            app.query_file(),
            app.term_lexicon(),
            app.stemmer(),
            app.stop_words(),
            min_query_len,
            max_query_len);
        return 0;
    } catch (std::runtime_error const& err) {
        spdlog::error(err.what());
        return 1;
    }
}
