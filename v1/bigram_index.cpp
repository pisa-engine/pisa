#include <fstream>
#include <iostream>
#include <optional>

#include <CLI/CLI.hpp>
#include <spdlog/spdlog.h>

#include "io.hpp"
#include "payload_vector.hpp"
#include "query/queries.hpp"
#include "timer.hpp"
#include "topk_queue.hpp"
#include "v1/blocked_cursor.hpp"
#include "v1/cursor_intersection.hpp"
#include "v1/index_builder.hpp"
#include "v1/index_metadata.hpp"
#include "v1/query.hpp"
#include "v1/raw_cursor.hpp"
#include "v1/scorer/bm25.hpp"
#include "v1/scorer/runner.hpp"
#include "v1/types.hpp"

using pisa::Payload_Vector_Buffer;
using pisa::v1::BigramMetadata;
using pisa::v1::BlockedReader;
using pisa::v1::CursorTraits;
using pisa::v1::DefaultProgress;
using pisa::v1::DocId;
using pisa::v1::Frequency;
using pisa::v1::index_runner;
using pisa::v1::IndexMetadata;
using pisa::v1::intersect;
using pisa::v1::PostingBuilder;
using pisa::v1::ProgressStatus;
using pisa::v1::RawReader;
using pisa::v1::resolve_yml;
using pisa::v1::TermId;
using pisa::v1::write_span;

auto collect_unique_bigrams(std::vector<pisa::Query> const& queries)
    -> std::vector<std::pair<TermId, TermId>>
{
    std::vector<std::pair<TermId, TermId>> bigrams;
    for (auto&& query : queries) {
        for (auto left = 0; left < query.terms.size(); left += 1) {
            auto right = left + 1;
            bigrams.emplace_back(query.terms[left], query.terms[right]);
        }
    }
    std::sort(bigrams.begin(), bigrams.end());
    bigrams.erase(std::unique(bigrams.begin(), bigrams.end()), bigrams.end());
    return bigrams;
}

int main(int argc, char** argv)
{
    std::optional<std::string> yml{};
    std::optional<std::string> query_file{};
    std::optional<std::string> terms_file{};

    CLI::App app{"Creates a v1 bigram index."};
    app.add_option("-i,--index",
                   yml,
                   "Path of .yml file of an index "
                   "(if not provided, it will be looked for in the current directory)",
                   false);
    app.add_option("-q,--query", query_file, "Path to file with queries", false);
    app.add_option("--terms", terms_file, "Overrides document lexicon from .yml (if defined).");
    CLI11_PARSE(app, argc, argv);

    auto resolved_yml = resolve_yml(yml);
    auto meta = IndexMetadata::from_file(resolved_yml);
    auto stemmer = meta.stemmer ? std::make_optional(*meta.stemmer) : std::optional<std::string>{};
    if (meta.term_lexicon) {
        terms_file = meta.term_lexicon.value();
    }

    std::vector<pisa::Query> queries;
    auto parse_query = resolve_query_parser(queries, terms_file, std::nullopt, stemmer);
    if (query_file) {
        std::ifstream is(*query_file);
        pisa::io::for_each_line(is, parse_query);
    } else {
        pisa::io::for_each_line(std::cin, parse_query);
    }

    auto bigrams = collect_unique_bigrams(queries);

    auto index_basename = resolved_yml.substr(0, resolved_yml.size() - 4);
    auto run = index_runner(meta,
                            RawReader<std::uint32_t>{},
                            BlockedReader<::pisa::simdbp_block, true>{},
                            BlockedReader<::pisa::simdbp_block, false>{});

    std::vector<std::array<TermId, 2>> pair_mapping;
    auto documents_file = fmt::format("{}.bigram_documents", index_basename);
    auto frequencies_file_0 = fmt::format("{}.bigram_frequencies_0", index_basename);
    auto frequencies_file_1 = fmt::format("{}.bigram_frequencies_1", index_basename);
    auto document_offsets_file = fmt::format("{}.bigram_document_offsets", index_basename);
    auto frequency_offsets_file_0 = fmt::format("{}.bigram_frequency_offsets_0", index_basename);
    auto frequency_offsets_file_1 = fmt::format("{}.bigram_frequency_offsets_1", index_basename);
    std::ofstream document_out(documents_file);
    std::ofstream frequency_out_0(frequencies_file_0);
    std::ofstream frequency_out_1(frequencies_file_1);

    run([&](auto&& index) {
        ProgressStatus status(bigrams.size(),
                              DefaultProgress("Building bigram index"),
                              std::chrono::milliseconds(100));
        using index_type = std::decay_t<decltype(index)>;
        using document_writer_type =
            typename CursorTraits<typename index_type::document_cursor_type>::Writer;
        using frequency_writer_type =
            typename CursorTraits<typename index_type::payload_cursor_type>::Writer;

        PostingBuilder<DocId> document_builder(document_writer_type{});
        PostingBuilder<Frequency> frequency_builder_0(frequency_writer_type{});
        PostingBuilder<Frequency> frequency_builder_1(frequency_writer_type{});

        document_builder.write_header(document_out);
        frequency_builder_0.write_header(frequency_out_0);
        frequency_builder_1.write_header(frequency_out_1);

        for (auto [left_term, right_term] : bigrams) {
            auto intersection = intersect({index.cursor(left_term), index.cursor(right_term)},
                                          std::array<Frequency, 2>{0, 0},
                                          [](auto& payload, auto& cursor, auto list_idx) {
                                              payload[list_idx] = cursor.payload();
                                              return payload;
                                          });
            if (intersection.empty()) {
                status += 1;
                continue;
            }
            pair_mapping.push_back({left_term, right_term});
            for_each(intersection, [&](auto& cursor) {
                document_builder.accumulate(*cursor);
                auto payload = cursor.payload();
                frequency_builder_0.accumulate(std::get<0>(payload));
                frequency_builder_1.accumulate(std::get<1>(payload));
            });
            document_builder.flush_segment(document_out);
            frequency_builder_0.flush_segment(frequency_out_0);
            frequency_builder_1.flush_segment(frequency_out_1);
            status += 1;
        }
        std::cerr << "Writing offsets...";
        write_span(gsl::make_span(document_builder.offsets()), document_offsets_file);
        write_span(gsl::make_span(frequency_builder_0.offsets()), frequency_offsets_file_0);
        write_span(gsl::make_span(frequency_builder_1.offsets()), frequency_offsets_file_1);
        std::cerr << " Done.\n";
    });
    std::cerr << "Writing metadata...";
    meta.bigrams = BigramMetadata{
        .documents = {.postings = documents_file, .offsets = document_offsets_file},
        .frequencies = {{.postings = frequencies_file_0, .offsets = frequency_offsets_file_0},
                        {.postings = frequencies_file_1, .offsets = frequency_offsets_file_1}},
        .mapping = fmt::format("{}.bigram_mapping", index_basename),
        .count = pair_mapping.size()};
    meta.write(resolved_yml);
    std::cerr << " Done.\nWriting bigram mapping...";
    Payload_Vector_Buffer::make(pair_mapping.begin(),
                                pair_mapping.end(),
                                [](auto&& terms, auto out) {
                                    auto bytes = gsl::as_bytes(gsl::make_span(terms));
                                    std::copy(bytes.begin(), bytes.end(), out);
                                })
        .to_file(meta.bigrams->mapping);
    std::cerr << " Done.\n";
    return 0;
}
