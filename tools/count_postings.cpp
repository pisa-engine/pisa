#include <iostream>
#include <numeric>
#include <vector>

#include <CLI/CLI.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "index_types.hpp"

using namespace pisa;

template <typename Index>
void extract(
    Index const* index_ptr,
    std::vector<Query> const& queries,
    std::string const& separator,
    bool sum,
    bool print_qid
) {
    auto const& index = *index_ptr;
    auto body = [&] {
        if (sum) {
            return std::function<void(Query const&)>([&](auto const& query) {
                auto count = std::accumulate(
                    query.terms().begin(), query.terms().end(), 0, [&](auto s, auto term) {
                        return s + index[term.id].size();
                    }
                );
                std::cout << count << '\n';
            });
        }
        return std::function<void(Query const&)>([&](auto const& query) {
            std::cout << boost::algorithm::join(
                query.terms() | boost::adaptors::transformed([&index](auto term) {
                    return std::to_string(index[term.id].size());
                }),
                separator
            );
            std::cout << '\n';
        });
    }();
    for (auto const& query: queries) {
        if (print_qid && query.id()) {
            std::cout << *query.id() << ":";
        }
        body(query);
    }
}

int main(int argc, char** argv) {
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    bool sum = false;

    App<arg::Index, arg::Query<arg::QueryMode::Unranked>, arg::Separator, arg::PrintQueryId, arg::LogLevel>
        app{"Extracts posting counts from an inverted index."};
    app.add_flag(
        "--sum",
        sum,
        "Sum postings accross the query terms; by default, individual list lengths will be "
        "printed, separated by the separator defined with --sep"
    );
    CLI11_PARSE(app, argc, argv);

    spdlog::set_level(app.log_level());

    run_for_index(
        app.index_encoding(), MemorySource::mapped_file(app.index_filename()), [&](auto index) {
            using Index = std::decay_t<decltype(index)>;
            extract<Index>(&index, app.queries(), app.separator(), sum, app.print_query_id());
        }
    );

    return 0;
}
