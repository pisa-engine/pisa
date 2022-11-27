#include <iostream>
#include <numeric>
#include <vector>

#include <CLI/CLI.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "binary_collection.hpp"
#include "index_types.hpp"

using namespace pisa;

template <typename Index>
void extract(
    std::string const& index_filename,
    std::vector<pisa::Query> const& queries,
    std::string const& separator,
    bool sum,
    bool print_qid)
{
    Index index(MemorySource::mapped_file(index_filename));
    auto body = [&] {
        if (sum) {
            return std::function<void(Query const&)>([&](auto const& query) {
                auto count = std::accumulate(
                    query.terms.begin(), query.terms.end(), 0, [&](auto s, auto term_id) {
                        return s + index[term_id].size();
                    });
                std::cout << count << '\n';
            });
        }
        return std::function<void(Query const&)>([&](auto const& query) {
            std::cout << boost::algorithm::join(
                query.terms | boost::adaptors::transformed([&index](auto term_id) {
                    return std::to_string(index[term_id].size());
                }),
                separator);
            std::cout << '\n';
        });
    }();
    for (auto const& query: queries) {
        if (print_qid && query.id) {
            std::cout << *query.id << ":";
        }
        body(query);
    }
}

int main(int argc, char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    bool sum = false;

    App<arg::Index, arg::Query<arg::QueryMode::Unranked>, arg::Separator, arg::PrintQueryId, arg::LogLevel>
        app{"Extracts posting counts from an inverted index."};
    app.add_flag(
        "--sum",
        sum,
        "Sum postings accross the query terms; by default, individual list lengths will be "
        "printed, separated by the separator defined with --sep");
    CLI11_PARSE(app, argc, argv);

    spdlog::set_level(app.log_level());

    /**/
    if (false) {
#define LOOP_BODY(R, DATA, T)                                                                 \
    }                                                                                         \
    else if (app.index_encoding() == BOOST_PP_STRINGIZE(T))                                   \
    {                                                                                         \
        extract<BOOST_PP_CAT(T, _index)>(                                                     \
            app.index_filename(), app.queries(), app.separator(), sum, app.print_query_id()); \
        /**/
        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY

    } else {
        spdlog::error("Unknown type {}", app.index_encoding());
    }

    return 0;
}
