#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptor/transformed.hpp>

#include "CLI/CLI.hpp"
#include "query/queries.hpp"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"

using namespace pisa;

int main(int argc, const char **argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    std::string query_filename;
    std::string terms_filename;
    std::optional<std::string> stopwords_filename;
    std::optional<std::string> stemmer = std::nullopt;
    std::string separator = "\t";
    bool query_id = false;

    CLI::App app{"A tool for transforming textual queries to IDs."};
    app.add_option("-q,--query", query_filename, "Queries filename")->required();
    app.add_option("-t,--terms", terms_filename, "Terms lexicon")->required();
    app.add_option("--stemmer", stemmer, "Stemmer type");
    app.add_option("--stopwords", stopwords_filename, "File containing stopwords to ignore");
    app.add_option("--sep", separator, "Separator");
    app.add_flag("--query-id", query_id, "Print query ID (as id:T1 T2 ... TN)");

    CLI11_PARSE(app, argc, argv);

    std::vector<Query> queries;
    auto parse_query = resolve_query_parser(queries, terms_filename, stopwords_filename, stemmer);
    std::ifstream is(query_filename);
    io::for_each_line(is, parse_query);

    using boost::adaptors::transformed;
    using boost::algorithm::join;
    for (auto &&q : queries) {
        if(query_id and q.id) {
            std::cout << *(q.id) << ":";
        }
        std::cout
            << join(q.terms | transformed([](auto d) { return std::to_string(d); }), separator)
            << std::endl;
    }
}
