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

    App<arg::Query<arg::QueryMode::Unranked>, arg::Separator, arg::PrintQueryId, arg::LogLevel> app{
        "A tool for transforming textual queries to IDs."};
    CLI11_PARSE(app, argc, argv);

    spdlog::set_level(app.log_level());

    using boost::adaptors::transformed;
    using boost::algorithm::join;
    for (auto&& q: app.queries()) {
        if (app.print_query_id() and q.id) {
            std::cout << *(q.id) << ":";
        }
        std::cout
            << join(q.terms | transformed([](auto d) { return std::to_string(d); }), app.separator())
            << '\n';
    }
}
