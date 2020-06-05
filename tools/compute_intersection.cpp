#include <limits>
#include <optional>
#include <string>
#include <vector>

#include "mappable/mapper.hpp"
#include <CLI/CLI.hpp>
#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <range/v3/view/filter.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "index_types.hpp"
#include "intersection.hpp"
#include "pisa/cursor/scored_cursor.hpp"
#include "pisa/query/queries.hpp"
#include "wand_data_raw.hpp"

using namespace pisa;
using pisa::intersection::IntersectionType;
using pisa::intersection::Mask;

template <typename IndexType, typename WandType, typename QueryRange>
void intersect(
    std::string const& index_filename,
    std::optional<std::string> const& wand_data_filename,
    QueryRange&& queries,
    IntersectionType intersection_type,
    std::optional<int> max_term_count)
{
    IndexType index;
    mio::mmap_source m(index_filename.c_str());
    mapper::map(index, m);

    WandType wdata;

    mio::mmap_source md;
    if (wand_data_filename) {
        std::error_code error;
        md.map(*wand_data_filename, error);
        if (error) {
            spdlog::error("error mapping file: {}, exiting...", error.message());
            std::abort();
        }
        mapper::map(wdata, md, mapper::map_flags::warmup);
    }

    for (auto&& query: queries) {
        if (intersection_type == IntersectionType::Combinations) {
            auto intersections = nlohmann::json::array();
            auto process_intersection = [&](auto const& query, auto const& mask) {
                auto intersection = Intersection::compute(index, wdata, query, mask);
                intersections.push_back(nlohmann::json{{"length", intersection.length},
                                                       {"max_score", intersection.max_score},
                                                       {"mask", mask.to_ulong()}});
            };
            for_all_subsets(query, max_term_count, process_intersection);
            auto output =
                nlohmann::json{{"query", query.to_json()}, {"intersections", intersections}};
            std::cout << output.dump() << '\n';
        } else {
            auto intersection = Intersection::compute(index, wdata, query);
            auto query_json = query.to_json();
            auto intersection_json = nlohmann::json::object();
            intersection_json["length"] = intersection.length;
            intersection_json["max_score"] = intersection.max_score;
            intersection_json["mask"] = (1U << query.term_ids()->size()) - 1;
            auto output = nlohmann::json{
                {"query", query_json}, {"intersections", nlohmann::json::array({intersection_json})}};
            std::cout << output.dump() << '\n';
        }
    }
}

using wand_raw_index = wand_data<wand_data_raw>;

int main(int argc, const char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    std::optional<int> max_term_count;
    std::size_t min_query_len = 0;
    std::size_t max_query_len = std::numeric_limits<std::size_t>::max();
    bool combinations = false;
    // bool header = false;

    App<arg::Index, arg::WandData<arg::WandMode::Required>, arg::Query<arg::QueryMode::Unranked>> app{
        "Computes intersections of posting lists."};
    auto* combinations_flag = app.add_flag(
        "--combinations", combinations, "Compute intersections for combinations of terms in query");
    app.add_option(
           "--max-term-count,--mtc",
           max_term_count,
           "Max number of terms when computing combinations")
        ->needs(combinations_flag);
    app.add_option("--min-query-len", min_query_len, "Minimum query length");
    app.add_option("--max-query-len", max_query_len, "Maximum query length");
    CLI11_PARSE(app, argc, argv);

    auto queries = app.resolved_queries();
    auto filtered_queries = ranges::views::filter(queries, [&](auto&& query) {
        auto size = query.term_ids()->size();
        return size >= min_query_len || size <= max_query_len;
    });

    IntersectionType intersection_type =
        combinations ? IntersectionType::Combinations : IntersectionType::Query;

    /**/
    if (false) {
#define LOOP_BODY(R, DATA, T)                               \
    }                                                       \
    else if (app.index_encoding() == BOOST_PP_STRINGIZE(T)) \
    {                                                       \
        intersect<BOOST_PP_CAT(T, _index), wand_raw_index>( \
            app.index_filename(),                           \
            app.wand_data_path(),                           \
            filtered_queries,                               \
            intersection_type,                              \
            max_term_count);                                \
        /**/

        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY

    } else {
        spdlog::error("Unknown type {}", app.index_encoding());
    }
}
