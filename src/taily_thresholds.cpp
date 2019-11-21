#include <iostream>
#include <optional>

#include "mio/mmap.hpp"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"

#include "mappable/mapper.hpp"

#include "io.hpp"
#include "query/queries.hpp"
#include "util/util.hpp"
#include "taily.hpp"

#include "CLI/CLI.hpp"

using namespace pisa;

void thresholds(const std::string &taily_stats_filename,
                const std::vector<Query> &queries,
                uint64_t k)
{
    std::ifstream ifs(taily_stats_filename);
    int64_t collection_size;
    ifs.read(reinterpret_cast<char*>(&collection_size), sizeof(collection_size));
    int64_t term_num;
    ifs.read(reinterpret_cast<char*>(&term_num), sizeof(term_num));

    std::vector<taily::Feature_Statistics> stats;
    for (int i = 0; i < term_num; ++i)
    {
        stats.push_back(taily::Feature_Statistics::from_stream(ifs));
    }


    for (auto const &query : queries) {
        std::vector<taily::Feature_Statistics> term_stats;
        auto terms = query.terms;
        for(auto&& t : terms) {
            term_stats.push_back(stats[t]);
        }
        taily::Query_Statistics query_stats{term_stats, collection_size};
        auto threshold = taily::estimate_cutoff(query_stats, k);
        std::cout << threshold << '\n';
    }
}


int main(int argc, const char **argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    std::string taily_stats_filename;
    std::optional<std::string> terms_file;
    std::optional<std::string> query_filename;
    std::optional<std::string> stemmer = std::nullopt;
    uint64_t k = configuration::get().k;

    CLI::App app{"A tool for predicting thresholds for queries using Taily."};
    app.set_config("--config", "", "Configuration .ini file", false);
    app.add_option("-t,--taily", taily_stats_filename, "Taily stats filename")->required();
    app.add_option("-q,--query", query_filename, "Queries filename");
    app.add_option("-k", k, "k value");
    auto *terms_opt =
        app.add_option("--terms", terms_file, "Text file with terms in separate lines");
    app.add_option("--stemmer", stemmer, "Stemmer type")->needs(terms_opt);
    CLI11_PARSE(app, argc, argv);

    std::vector<Query> queries;
    auto parse_query = resolve_query_parser(queries, terms_file, std::nullopt, stemmer);
    if (query_filename) {
        std::ifstream is(*query_filename);
        io::for_each_line(is, parse_query);
    } else {
        io::for_each_line(std::cin, parse_query);
    }

    thresholds(taily_stats_filename, queries, k);
}
