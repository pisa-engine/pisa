#include <limits>
#include <optional>
#include <string>
#include <vector>

#include <CLI/CLI.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <tl/optional.hpp>

#include "app.hpp"
#include "intersection.hpp"
#include "query/queries.hpp"
#include "v1/blocked_cursor.hpp"
#include "v1/cursor/accumulate.hpp"
#include "v1/cursor/for_each.hpp"
#include "v1/cursor_intersection.hpp"
#include "v1/default_index_runner.hpp"
#include "v1/raw_cursor.hpp"
#include "v1/scorer/bm25.hpp"

using pisa::App;
using pisa::Intersection;
using pisa::intersection::IntersectionType;
using pisa::intersection::Mask;
using pisa::v1::intersect;
using pisa::v1::make_bm25;
using pisa::v1::RawReader;

namespace arg = pisa::arg;
namespace v1 = pisa::v1;

template <typename Index>
auto compute_intersection(Index const& index,
                          pisa::v1::Query const& query,
                          tl::optional<std::bitset<64>> term_selection) -> Intersection
{
    auto const term_ids =
        term_selection ? query.filtered_terms(*term_selection) : query.get_term_ids();
    if (term_ids.size() == 1) {
        auto cursor = index.max_scored_cursor(term_ids[0], make_bm25(index));
        return Intersection{cursor.size(), cursor.max_score()};
    }
    auto cursors = index.scored_cursors(gsl::make_span(term_ids), make_bm25(index));
    auto intersection =
        intersect(cursors, 0.0F, [](auto score, auto&& cursor, [[maybe_unused]] auto idx) {
            return score + cursor.payload();
        });
    std::size_t postings = 0;
    float max_score = 0.0F;
    v1::for_each(intersection, [&](auto& cursor) {
        postings += 1;
        if (auto score = cursor.payload(); score > max_score) {
            max_score = score;
        }
    });
    return Intersection{postings, max_score};
}

/// Do `func` for all intersections in a query that have a given maximum number of terms.
/// `Fn` takes `Query` and `Mask`.
template <typename Fn>
auto for_all_subsets(v1::Query const& query, tl::optional<std::size_t> max_term_count, Fn func)
{
    auto&& term_ids = query.get_term_ids();
    auto subset_count = 1U << term_ids.size();
    for (auto subset = 1U; subset < subset_count; ++subset) {
        auto mask = std::bitset<64>(subset);
        if (!max_term_count || mask.count() <= *max_term_count) {
            func(query, mask);
        }
    }
}

template <typename Index, typename QRng>
void compute_intersections(Index const& index,
                           QRng queries,
                           IntersectionType intersection_type,
                           tl::optional<std::size_t> max_term_count,
                           bool existing)
{
    for (auto const& query : queries) {
        auto intersections = nlohmann::json::array();
        auto inter = [&](auto&& query, tl::optional<std::bitset<64>> const& mask) {
            auto intersection = compute_intersection(index, query, mask);
            if (intersection.length > 0) {
                intersections.push_back(
                    nlohmann::json{{"intersection", mask.value_or(0).to_ulong()},
                                   {"cost", intersection.length},
                                   {"max_score", intersection.max_score}});
            }
        };
        if (intersection_type == IntersectionType::Combinations) {
            if (existing) {
                std::uint64_t left_mask = 1;
                auto term_ids = query.get_term_ids();
                for (auto left = 0; left < term_ids.size(); left += 1) {
                    auto cursor = index.max_scored_cursor(term_ids[left], make_bm25(index));
                    intersections.push_back(nlohmann::json{{"intersection", left_mask},
                                                           {"cost", cursor.size()},
                                                           {"max_score", cursor.max_score()}});
                    std::uint64_t right_mask = left_mask << 1U;
                    for (auto right = left + 1; right < term_ids.size(); right += 1) {
                        index
                            .scored_bigram_cursor(term_ids[left], term_ids[right], make_bm25(index))
                            .map([&](auto&& cursor) {
                                // TODO(michal): Do not traverse once max scores for bigrams are
                                //               implemented
                                auto cost = cursor.size();
                                auto max_score =
                                    accumulate(cursor, 0.0F, [](float acc, auto&& cursor) {
                                        auto score = std::get<0>(cursor.payload())
                                                     + std::get<1>(cursor.payload());
                                        return std::max(acc, score);
                                    });
                                intersections.push_back(
                                    nlohmann::json{{"intersection", left_mask | right_mask},
                                                   {"cost", cost},
                                                   {"max_score", max_score}});
                            });
                        right_mask <<= 1U;
                    }
                    left_mask <<= 1U;
                }
            } else {
                for_all_subsets(query, max_term_count, inter);
            }
        } else {
            inter(query, tl::nullopt);
        }
        auto output = *query.to_json();
        output["intersections"] = intersections;
        std::cout << output << '\n';
    }
}

int main(int argc, const char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    bool combinations = false;
    bool existing = false;
    std::optional<std::size_t> max_term_count;

    pisa::App<arg::Index, arg::Query<arg::QueryMode::Unranked>> app(
        "Calculates intersections for a v1 index.");
    auto* combinations_flag = app.add_flag(
        "--combinations", combinations, "Compute intersections for combinations of terms in query");
    auto* mtc_flag = app.add_option("--max-term-count,--mtc",
                                    max_term_count,
                                    "Max number of terms when computing combinations");
    mtc_flag->needs(combinations_flag);
    app.add_flag("--existing", existing, "Use only existing bigrams")
        ->needs(combinations_flag)
        ->excludes(mtc_flag);
    CLI11_PARSE(app, argc, argv);
    auto mtc = max_term_count ? tl::make_optional(*max_term_count) : tl::optional<std::size_t>{};

    IntersectionType intersection_type =
        combinations ? IntersectionType::Combinations : IntersectionType::Query;

    try {
        auto meta = app.index_metadata();
        auto queries = app.query_range(meta);

        auto run = index_runner(meta);
        run([&](auto&& index) {
            compute_intersections(index, queries, intersection_type, mtc, existing);
        });
    } catch (std::exception const& error) {
        spdlog::error("{}", error.what());
    }
    return 0;
}
