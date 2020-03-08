#include <CLI/CLI.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptor/transformed.hpp>

#include "app.hpp"
#include "query/queries.hpp"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"

using namespace pisa;

int main(int argc, const char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    std::string separator = "\t";
    bool query_id = false;

    App<arg::Query<arg::QueryMode::Unranked>> app{
        "A tool for transforming textual queries to IDs."};
    app.add_option("--sep", separator, "Separator");
    app.add_flag("--query-id", query_id, "Print query ID (as id:T1 T2 ... TN)");
    CLI11_PARSE(app, argc, argv);

    using boost::adaptors::transformed;
    using boost::algorithm::join;
    for (auto&& q: app.queries()) {
        if (query_id and q.id) {
            std::cout << *(q.id) << ":";
        }
        std::cout << join(q.terms | transformed([](auto d) { return std::to_string(d); }), separator)
                  << '\n';
    }
}
