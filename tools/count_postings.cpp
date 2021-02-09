#include <iostream>
#include <numeric>
#include <vector>

#include <CLI/CLI.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "binary_collection.hpp"
#include "index_types.hpp"

using namespace pisa;

template <typename Index>
void extract(std::string const& index_filename, std::vector<pisa::Query> const& queries)
{
    Index index(MemorySource::mapped_file(index_filename));
    auto accumulate_size = [&](auto sum, auto term_id) { return sum + index[term_id].size(); };
    for (auto const& query: queries) {
        auto count = std::accumulate(query.terms.begin(), query.terms.end(), 0, accumulate_size);
        std::cout << count << '\n';
    }
}

int main(int argc, char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    App<arg::Index, arg::Query<arg::QueryMode::Unranked>> app{
        "Extracts posting counts from an inverted index."};
    CLI11_PARSE(app, argc, argv);

    /**/
    if (false) {
#define LOOP_BODY(R, DATA, T)                                                  \
    }                                                                          \
    else if (app.index_encoding() == BOOST_PP_STRINGIZE(T))                    \
    {                                                                          \
        extract<BOOST_PP_CAT(T, _index)>(app.index_filename(), app.queries()); \
        /**/
        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY

    } else {
        spdlog::error("Unknown type {}", app.index_encoding());
    }

    return 0;
}
