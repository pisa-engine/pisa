#include <iostream>

#include "app.hpp"
#include "cursor/cursor.hpp"
#include "index_types.hpp"
#include "query/algorithm/and_query.hpp"
#include "query/algorithm/or_query.hpp"
#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptor/transformed.hpp>

using namespace pisa;

template <typename IndexType>
void selective_queries(
    IndexType const* index_ptr, std::string const& encoding, std::vector<Query> const& queries
) {
    auto const& index = *index_ptr;

    spdlog::info("Performing {} queries", encoding);

    using boost::adaptors::transformed;
    using boost::algorithm::join;
    for (auto const& query: queries) {
        size_t and_results = and_query()(make_cursors(index, query), index.num_docs()).size();
        size_t or_results = or_query<false>()(make_cursors(index, query), index.num_docs());

        double selectiveness = double(and_results) / double(or_results);
        if (selectiveness < 0.005) {
            std::cout
                << join(query.terms() | transformed([](auto d) { return std::to_string(d.id); }), " ")
                << '\n';
        }
    }
}

int main(int argc, const char** argv) {
    App<arg::Index, arg::Query<arg::QueryMode::Unranked>, arg::LogLevel> app{
        "Filters selective queries for a given index."
    };
    CLI11_PARSE(app, argc, argv);

    spdlog::set_level(app.log_level());

    run_for_index(
        app.index_encoding(), MemorySource::mapped_file(app.index_filename()), [&](auto index) {
            using Index = std::decay_t<decltype(index)>;
            selective_queries<Index>(&index, app.index_encoding(), app.queries());
        }
    );
}
