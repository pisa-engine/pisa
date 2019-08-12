#include <iostream>
#include <optional>
#include <string>
#include <vector>

#include <CLI/CLI.hpp>
#include <spdlog/sinks/null_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "query/queries.hpp"

namespace pisa {

void perftest(const std::string &index_filename,
              const std::optional<std::string> &wand_data_filename,
              const std::vector<Query> &queries,
              const std::optional<std::string> &thresholds_filename,
              std::string const &type,
              std::string const &query_type,
              uint64_t k,
              std::string const &scorer_name,
              bool extract,
              bool compressed);

}

using namespace pisa;

int main(int argc, const char **argv)
{
    std::string type;
    std::string query_type;
    std::string index_filename;
    std::string scorer_name;
    std::optional<std::string> terms_file;
    std::optional<std::string> wand_data_filename;
    std::optional<std::string> query_filename;
    std::optional<std::string> thresholds_filename;
    std::optional<std::string> stopwords_filename;
    std::optional<std::string> stemmer = std::nullopt;
    uint64_t k = 1000;
    bool compressed = false;
    bool extract = false;
    bool silent = false;

    CLI::App app{"queries - a tool for performing queries on an index."};
    app.set_config("--config", "", "Configuration .ini file", false);
    app.add_option("-t,--type", type, "Index type")->required();
    app.add_option("-a,--algorithm", query_type, "Query algorithm")->required();
    app.add_option("-i,--index", index_filename, "Collection basename")->required();
    app.add_option("-w,--wand", wand_data_filename, "Wand data filename");
    app.add_option("-q,--query", query_filename, "Queries filename");
    app.add_option("-s,--scorer", scorer_name, "Scorer function")->required();
    app.add_flag("--compressed-wand", compressed, "Compressed wand input file");
    app.add_option("-k", k, "k value");
    app.add_option("--stopwords", stopwords_filename, "File containing stopwords to ignore");
    app.add_option("-T,--thresholds", thresholds_filename, "k value");
    auto *terms_opt = app.add_option("--terms", terms_file, "Term lexicon");
    app.add_option("--stemmer", stemmer, "Stemmer type")->needs(terms_opt);
    app.add_flag("--extract", extract, "Extract individual query times");
    app.add_flag("--silent", silent, "Suppress logging");
    CLI11_PARSE(app, argc, argv);

    if (silent) {
        spdlog::set_default_logger(spdlog::create<spdlog::sinks::null_sink_mt>("stderr"));
    } else {
        spdlog::set_default_logger(spdlog::stderr_color_mt("stderr"));
    }

    auto process_term = query::term_processor(terms_file, stemmer);

    std::unordered_set<term_id_type> stopwords;
    if (stopwords_filename) {
        std::ifstream is(*stopwords_filename);
        io::for_each_line(is, [&](auto &&word) {
            if (auto processed_term = process_term(std::move(word)); process_term) {
                stopwords.insert(*processed_term);
            }
        });
    }

    std::vector<Query> queries;
    auto push_query = [&](std::string const &query_line) {
        queries.push_back(parse_query(query_line, process_term, stopwords));
    };

    if (extract) {
        std::cout << "qid\tusec\n";
    }

    if (query_filename) {
        std::ifstream is(*query_filename);
        io::for_each_line(is, push_query);
    } else {
        io::for_each_line(std::cin, push_query);
    }

    perftest(index_filename,
             wand_data_filename,
             queries,
             thresholds_filename,
             type,
             query_type,
             k,
             scorer_name,
             extract,
             compressed);
}
