#include <fstream>
#include <iostream>
#include <optional>
#include <tuple>

#include <CLI/CLI.hpp>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "io.hpp"
#include "query/queries.hpp"
#include "topk_queue.hpp"
#include "v1/blocked_cursor.hpp"
#include "v1/cursor/collect.hpp"
#include "v1/cursor_accumulator.hpp"
#include "v1/cursor_intersection.hpp"
#include "v1/default_index_runner.hpp"
#include "v1/index_metadata.hpp"
#include "v1/query.hpp"
#include "v1/raw_cursor.hpp"
#include "v1/runtime_assert.hpp"
#include "v1/scorer/bm25.hpp"
#include "v1/scorer/runner.hpp"
#include "v1/types.hpp"

using pisa::App;
using pisa::Query;
using pisa::resolve_query_parser;
using pisa::v1::collect_payloads;
using pisa::v1::index_runner;
using pisa::v1::runtime_assert;

namespace arg = pisa::arg;

[[nodiscard]] auto load_source(std::optional<std::string> const& file)
    -> std::shared_ptr<mio::mmap_source>
{
    if (file) {
        return std::make_shared<mio::mmap_source>(file->c_str());
    }
    return nullptr;
}

[[nodiscard]] auto load_payload_vector(std::shared_ptr<mio::mmap_source> const& source)
    -> std::optional<pisa::Payload_Vector<>>
{
    if (source) {
        return pisa::Payload_Vector<>::from(*source);
    }
    return std::nullopt;
}

/// Returns the first value (not nullopt), or nullopt if no optional contains a value.
template <typename First, typename... Optional>
[[nodiscard]] auto value(First&& first, Optional&&... candidtes)
{
    std::optional<std::decay_t<decltype(first.value())>> val = std::nullopt;
    auto has_value = [&](auto&& opt) -> bool {
        if (not val.has_value() && opt) {
            val = *opt;
            return true;
        }
        return false;
    };
    has_value(first) || (has_value(candidtes) || ...);
    return val;
}

void print_header(std::vector<int> const& percentiles, std::vector<std::size_t> const& cutoffs)
{
    std::cout << "length";
    for (auto cutoff : cutoffs) {
        std::cout << '\t' << "top-" << cutoff + 1;
    }
    for (auto percentile : percentiles) {
        std::cout << '\t' << "perc-" << percentile;
    }
    std::cout << '\n';
}

template <typename Cursor>
void calc_stats(Cursor&& cursor,
                std::vector<int> const& percentiles,
                std::vector<std::size_t> const& cutoffs)
{
    using payload_type = std::decay_t<decltype(cursor.payload())>;
    auto payloads = collect_payloads(cursor);
    auto length = payloads.size();
    std::sort(payloads.begin(), payloads.end(), std::greater<>{});
    auto kth = [&](auto k) {
        if (k < payloads.size()) {
            return payloads[k];
        }
        return payload_type{};
    };
    std::cout << length;
    for (auto cutoff : cutoffs) {
        std::cout << '\t' << kth(cutoff);
    }
    for (auto percentile : percentiles) {
        std::cout << '\t'
                  << payloads[std::min(percentile * payloads.size() / 100, payloads.size() - 1)];
    }
    std::cout << '\n';
}

template <typename Cursor, typename Scorer>
auto print_postings(Cursor&& cursor,
                    Scorer&& scorer,
                    std::optional<pisa::Payload_Vector<>> const& docmap,
                    bool did,
                    bool print_frequencies,
                    bool print_scores)
{
    auto print = [&](auto&& cursor) {
        if (did) {
            std::cout << *cursor;
        } else {
            std::cout << docmap.value()[*cursor];
        }
        if (print_frequencies) {
            std::cout << " " << cursor.payload();
        }
        if (print_scores) {
            std::cout << " " << scorer(cursor.value(), cursor.payload());
        }
        std::cout << '\n';
    };
    for_each(cursor, print);
};

template <typename Cursor>
auto print_precomputed_postings(Cursor&& cursor,
                                std::optional<pisa::Payload_Vector<>> const& docmap,
                                bool did)
{
    auto print = [&](auto&& cursor) {
        if (did) {
            std::cout << *cursor;
        } else {
            std::cout << docmap.value()[*cursor];
        }
        if constexpr (std::is_same_v<decltype(cursor.payload()), std::uint8_t>) {
            std::cout << " " << static_cast<int>(cursor.payload()) << '\n';
        } else {
            std::cout << " " << cursor.payload() << '\n';
        }
    };
    for_each(std::forward<Cursor>(cursor), print);
};

int main(int argc, char** argv)
{
    std::vector<int> percentiles = {0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
    std::vector<std::size_t> cutoffs = {
        0, 9, 99, 999, 9'999, 99'999, 999'999, 9'999'999, 99'999'999};
    std::optional<std::string> terms_file{};
    std::optional<std::string> documents_file{};
    std::string query_input{};
    bool tid = false;
    bool did = false;
    bool print_frequencies = false;
    bool print_scores = false;
    bool precomputed = false;
    bool stats = false;
    bool header = false;

    App<arg::Index> app{"Queries a v1 index."};
    app.add_option("--terms", terms_file, "Overrides document lexicon from .yml (if defined).");
    app.add_option("--documents",
                   documents_file,
                   "Overrides document lexicon from .yml (if defined). Required otherwise.");
    app.add_flag("--tid", tid, "Use term IDs instead of terms");
    app.add_flag("--did", did, "Print document IDs instead of titles");
    app.add_flag("-f,--frequencies", print_frequencies, "Print frequencies");
    auto* scores_option = app.add_flag("-s,--scores", print_scores, "Print BM25 scores");
    app.add_flag("--precomputed", precomputed, "Use BM25 precomputed scores")->needs(scores_option);

    auto* stats_option = app.add_flag("--stats", stats, "Print stats instead of listing postings");
    app.add_option("--percentiles", percentiles, "Percentiles for stats", true)
        ->needs(stats_option);
    app.add_option("--cutoffs", cutoffs, "Cut-offs for stats", true)->needs(stats_option);
    app.add_flag("--header", header, "Print stats header")->needs(stats_option);

    app.add_option("query", query_input, "List of terms", false)->required();
    CLI11_PARSE(app, argc, argv);

    auto meta = app.index_metadata();
    auto stemmer = meta.stemmer ? std::make_optional(*meta.stemmer) : std::optional<std::string>{};
    if (tid) {
        terms_file = std::nullopt;
    } else {
        terms_file = value(meta.term_lexicon, terms_file);
    }
    documents_file = value(meta.document_lexicon, documents_file);

    if (not did and not documents_file) {
        spdlog::error("Document lexicon not defined");
        std::exit(1);
    }

    if (not tid and not terms_file) {
        spdlog::error("Term lexicon not defined");
        std::exit(1);
    }

    std::shared_ptr<mio::mmap_source> const source = load_source(documents_file);
    std::optional<pisa::Payload_Vector<>> const docmap = load_payload_vector(source);

    auto const query = [&]() {
        std::vector<Query> queries;
        auto parse_query = resolve_query_parser(queries, terms_file, std::nullopt, stemmer);
        parse_query(query_input);
        return queries[0];
    }();

    if (header) {
        print_header(percentiles, cutoffs);
    }

    if (stats) {
        for (auto percentile : percentiles) {
            pisa::v1::runtime_assert(percentile >= 0 && percentile <= 100)
                .or_exit("Percentiles must be in [0, 100]");
        }
    }

    if (query.terms.size() == 1) {
        if (precomputed) {
            auto run = scored_index_runner(meta);
            run([&](auto&& index) {
                auto cursor = index.cursor(query.terms.front());
                if (stats) {
                    calc_stats(cursor, percentiles, cutoffs);
                } else {
                    print_precomputed_postings(cursor, docmap, did);
                }
            });
        } else {
            auto run = index_runner(meta);
            run([&](auto&& index) {
                auto bm25 = make_bm25(index);
                if (stats) {
                    calc_stats(
                        index.scored_cursor(query.terms.front(), bm25), percentiles, cutoffs);
                } else {
                    auto scorer = bm25.term_scorer(query.terms.front());
                    print_postings(index.cursor(query.terms.front()),
                                   scorer,
                                   docmap,
                                   did,
                                   print_frequencies,
                                   print_scores);
                }
            });
        }
    } else {
        if (precomputed) {
            auto run = scored_index_runner(meta);
            run([&](auto&& index) {
                auto cursor =
                    ::pisa::v1::intersect(index.cursors(gsl::make_span(query.terms)),
                                          0.0,
                                          [](auto acc, auto&& cursor, [[maybe_unused]] auto idx) {
                                              return acc + cursor.payload();
                                          });
                if (stats) {
                    calc_stats(cursor, percentiles, cutoffs);
                } else {
                    print_precomputed_postings(cursor, docmap, did);
                }
            });
        } else {
            auto run = index_runner(meta);
            run([&](auto&& index) {
                auto bm25 = make_bm25(index);
                if (stats) {
                    auto cursor = ::pisa::v1::intersect(
                        index.scored_cursors(gsl::make_span(query.terms), bm25),
                        0.0,
                        [](auto acc, auto&& cursor, [[maybe_unused]] auto idx) {
                            return acc + cursor.payload();
                        });
                    calc_stats(cursor, percentiles, cutoffs);
                } else {
                    runtime_assert(query.terms.size() == 1)
                        .or_exit("Printing scoring intersections not supported yet.");
                    auto scorer = bm25.term_scorer(query.terms.front());
                    print_postings(index.cursor(query.terms.front()),
                                   scorer,
                                   docmap,
                                   did,
                                   print_frequencies,
                                   print_scores);
                }
            });
        }
    }

    return 0;
}
