#include "v1/index_builder.hpp"
#include "codec/simdbp.hpp"
#include "v1/blocked_cursor.hpp"
#include "v1/query.hpp"
#include "v1/raw_cursor.hpp"

namespace pisa::v1 {

auto collect_unique_bigrams(std::vector<Query> const& queries,
                            std::function<void()> const& callback)
    -> std::vector<std::pair<TermId, TermId>>
{
    std::vector<std::pair<TermId, TermId>> bigrams;
    auto idx = 0;
    for (auto query : queries) {
        auto const& term_ids = query.get_term_ids();
        if (term_ids.empty()) {
            continue;
        }
        callback();
        for (auto left = 0; left < term_ids.size() - 1; left += 1) {
            for (auto right = left + 1; right < term_ids.size(); right += 1) {
                bigrams.emplace_back(term_ids[left], term_ids[right]);
            }
        }
    }
    std::sort(bigrams.begin(), bigrams.end());
    bigrams.erase(std::unique(bigrams.begin(), bigrams.end()), bigrams.end());
    return bigrams;
}

auto verify_compressed_index(std::string const& input, std::string_view output)
    -> std::vector<std::string>
{
    std::vector<std::string> errors;
    pisa::binary_freq_collection const collection(input.c_str());
    auto meta = IndexMetadata::from_file(fmt::format("{}.yml", output));
    auto run = index_runner(meta,
                            RawReader<std::uint32_t>{},
                            BlockedReader<::pisa::simdbp_block, true>{},
                            BlockedReader<::pisa::simdbp_block, false>{});
    ProgressStatus status(
        collection.size(), DefaultProgress("Verifying"), std::chrono::milliseconds(100));
    run([&](auto&& index) {
        auto sequence_iter = collection.begin();
        for (auto term = 0; term < index.num_terms(); term += 1, ++sequence_iter) {
            auto document_sequence = sequence_iter->docs;
            auto frequency_sequence = sequence_iter->freqs;
            auto cursor = index.cursor(term);
            if (cursor.size() != document_sequence.size()) {
                errors.push_back(
                    fmt::format("Posting list length mismatch for term {}: expected {} but is {}",
                                term,
                                document_sequence.size(),
                                cursor.size()));
                continue;
            }
            auto dit = document_sequence.begin();
            auto fit = frequency_sequence.begin();
            auto pos = 0;
            while (not cursor.empty()) {
                if (cursor.value() != *dit) {
                    errors.push_back(
                        fmt::format("Document mismatch for term {} at position {}", term, pos));
                }
                if (cursor.payload() != *fit) {
                    errors.push_back(
                        fmt::format("Frequency mismatch for term {} at position {}", term, pos));
                }
                cursor.advance();
                ++dit;
                ++fit;
                ++pos;
            }
            status += 1;
        }
    });
    return errors;
}

[[nodiscard]] auto build_scored_bigram_index(IndexMetadata meta,
                                             std::string const& index_basename,
                                             std::vector<std::pair<TermId, TermId>> const& bigrams)
    -> std::pair<PostingFilePaths, PostingFilePaths>
{
    auto run = scored_index_runner(meta,
                                   RawReader<std::uint32_t>{},
                                   RawReader<std::uint8_t>{},
                                   BlockedReader<::pisa::simdbp_block, true>{},
                                   BlockedReader<::pisa::simdbp_block, false>{});

    std::vector<std::array<TermId, 2>> pair_mapping;
    auto scores_file_0 = fmt::format("{}.bigram_bm25_0", index_basename);
    auto scores_file_1 = fmt::format("{}.bigram_bm25_1", index_basename);
    auto score_offsets_file_0 = fmt::format("{}.bigram_bm25_offsets_0", index_basename);
    auto score_offsets_file_1 = fmt::format("{}.bigram_bm25_offsets_1", index_basename);
    std::ofstream score_out_0(scores_file_0);
    std::ofstream score_out_1(scores_file_1);

    run([&](auto&& index) {
        ProgressStatus status(bigrams.size(),
                              DefaultProgress("Building scored index"),
                              std::chrono::milliseconds(100));
        using index_type = std::decay_t<decltype(index)>;
        using score_writer_type =
            typename CursorTraits<typename index_type::payload_cursor_type>::Writer;

        PostingBuilder<std::uint8_t> score_builder_0(score_writer_type{});
        PostingBuilder<std::uint8_t> score_builder_1(score_writer_type{});

        score_builder_0.write_header(score_out_0);
        score_builder_1.write_header(score_out_1);

        for (auto [left_term, right_term] : bigrams) {
            auto intersection = intersect({index.scored_cursor(left_term, VoidScorer{}),
                                           index.scored_cursor(right_term, VoidScorer{})},
                                          std::array<std::uint8_t, 2>{0, 0},
                                          [](auto& payload, auto& cursor, auto list_idx) {
                                              payload[list_idx] = cursor.payload();
                                              return payload;
                                          });
            if (intersection.empty()) {
                status += 1;
                continue;
            }
            for_each(intersection, [&](auto& cursor) {
                auto payload = cursor.payload();
                score_builder_0.accumulate(std::get<0>(payload));
                score_builder_1.accumulate(std::get<1>(payload));
            });
            score_builder_0.flush_segment(score_out_0);
            score_builder_1.flush_segment(score_out_1);
            status += 1;
        }
        write_span(gsl::make_span(score_builder_0.offsets()), score_offsets_file_0);
        write_span(gsl::make_span(score_builder_1.offsets()), score_offsets_file_1);
    });
    return {PostingFilePaths{scores_file_0, score_offsets_file_0},
            PostingFilePaths{scores_file_1, score_offsets_file_1}};
}

void build_bigram_index(std::string const& yml,
                        std::vector<std::pair<TermId, TermId>> const& bigrams)
{
    Expects(not bigrams.empty());
    auto index_basename = yml.substr(0, yml.size() - 4);
    auto meta = IndexMetadata::from_file(yml);
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
    meta.bigrams = BigramMetadata{
        .documents = {.postings = documents_file, .offsets = document_offsets_file},
        .frequencies = {{.postings = frequencies_file_0, .offsets = frequency_offsets_file_0},
                        {.postings = frequencies_file_1, .offsets = frequency_offsets_file_1}},
        .scores = {},
        .mapping = fmt::format("{}.bigram_mapping", index_basename),
        .count = pair_mapping.size()};
    if (not meta.scores.empty()) {
        meta.bigrams->scores.push_back(build_scored_bigram_index(meta, index_basename, bigrams));
    }

    std::cerr << "Writing metadata...";
    meta.write(yml);
    std::cerr << " Done.\nWriting bigram mapping...";
    write_span(gsl::make_span(pair_mapping), meta.bigrams->mapping);
    std::cerr << " Done.\n";
}

} // namespace pisa::v1
