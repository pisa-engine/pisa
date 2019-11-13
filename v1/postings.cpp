#include <fstream>
#include <iostream>
#include <optional>
#include <tuple>

#include <CLI/CLI.hpp>
#include <spdlog/spdlog.h>

#include "io.hpp"
#include "query/queries.hpp"
#include "topk_queue.hpp"
#include "v1/blocked_cursor.hpp"
#include "v1/index_metadata.hpp"
#include "v1/query.hpp"
#include "v1/raw_cursor.hpp"
#include "v1/scorer/bm25.hpp"
#include "v1/scorer/runner.hpp"
#include "v1/types.hpp"

using pisa::Query;
using pisa::resolve_query_parser;
using pisa::v1::BlockedReader;
using pisa::v1::index_runner;
using pisa::v1::IndexMetadata;
using pisa::v1::RawReader;
using pisa::v1::resolve_yml;

auto default_readers()
{
    return std::make_tuple(RawReader<std::uint32_t>{},
                           RawReader<std::uint8_t>{},
                           RawReader<float>{},
                           BlockedReader<::pisa::simdbp_block, true>{},
                           BlockedReader<::pisa::simdbp_block, false>{});
}

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

int main(int argc, char** argv)
{
    std::optional<std::string> yml{};
    std::optional<std::string> terms_file{};
    std::optional<std::string> documents_file{};
    std::string query_input{};
    bool tid = false;
    bool did = false;
    bool print_frequencies = false;
    bool print_scores = false;
    bool precomputed = false;

    CLI::App app{"Queries a v1 index."};
    app.add_option("-i,--index",
                   yml,
                   "Path of .yml file of an index "
                   "(if not provided, it will be looked for in the current directory)",
                   false);
    app.add_option("--terms", terms_file, "Overrides document lexicon from .yml (if defined).");
    app.add_option("--documents",
                   documents_file,
                   "Overrides document lexicon from .yml (if defined). Required otherwise.");
    app.add_flag("--tid", tid, "Use term IDs instead of terms");
    app.add_flag("--did", did, "Print document IDs instead of titles");
    app.add_flag("-f,--frequencies", print_frequencies, "Print frequencies");
    auto* scores_option = app.add_flag("-s,--scores", print_scores, "Print BM25 scores");
    app.add_flag("--precomputed", precomputed, "Use BM25 precomputed scores")->needs(scores_option);
    app.add_option("query", query_input, "List of terms", false)->required();
    CLI11_PARSE(app, argc, argv);

    auto meta = IndexMetadata::from_file(resolve_yml(yml));
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

    if (query.terms.size() == 1) {
        if (precomputed) {
            auto run = scored_index_runner(meta, default_readers());
            run([&](auto&& index) {
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
                for_each(index.cursor(query.terms.front()), print);
            });
        } else {
            auto run = index_runner(meta, default_readers());
            run([&](auto&& index) {
                auto bm25 = make_bm25(index);
                auto scorer = bm25.term_scorer(query.terms.front());
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
                for_each(index.cursor(query.terms.front()), print);
            });
        }
    } else {
        std::cerr << "Multiple terms unimplemented";
        std::exit(1);
    }

    return 0;
}
