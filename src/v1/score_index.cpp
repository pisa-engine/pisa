#include <exception>

#include <tbb/task_group.h>

#include "codec/simdbp.hpp"
#include "v1/blocked_cursor.hpp"
#include "v1/default_index_runner.hpp"
#include "v1/index_builder.hpp"
#include "v1/index_metadata.hpp"
#include "v1/progress_status.hpp"
#include "v1/raw_cursor.hpp"
#include "v1/score_index.hpp"
#include "v1/scorer/bm25.hpp"

using pisa::v1::DefaultProgress;
using pisa::v1::IndexMetadata;
using pisa::v1::PostingFilePaths;
using pisa::v1::ProgressStatus;
using pisa::v1::RawReader;
using pisa::v1::RawWriter;
using pisa::v1::TermId;
using pisa::v1::write_span;

namespace pisa::v1 {

auto score_index(IndexMetadata meta, std::size_t threads) -> IndexMetadata
{
    auto run = index_runner(meta);
    auto const& index_basename = meta.get_basename();
    auto postings_path = fmt::format("{}.bm25", index_basename);
    auto offsets_path = fmt::format("{}.bm25_offsets", index_basename);
    auto max_scores_path = fmt::format("{}.bm25.maxf", index_basename);
    auto quantized_max_scores_path = fmt::format("{}.bm25.maxq", index_basename);
    run([&](auto&& index) {
        ProgressStatus calc_max_status(index.num_terms(),
                                       DefaultProgress("Calculating max partial score"),
                                       std::chrono::milliseconds(100));
        std::vector<float> max_scores(index.num_terms(), 0.0F);
        tbb::task_group group;
        auto batch_size = index.num_terms() / threads;
        for (auto thread_id = 0; thread_id < threads; thread_id += 1) {
            auto first_term = thread_id * batch_size;
            auto end_term =
                thread_id < threads - 1 ? (thread_id + 1) * batch_size : index.num_terms();
            std::for_each(boost::counting_iterator<TermId>(first_term),
                          boost::counting_iterator<TermId>(end_term),
                          [&](auto term) {
                              for_each(
                                  index.scoring_cursor(term, make_bm25(index)), [&](auto&& cursor) {
                                      if (auto score = cursor.payload(); max_scores[term] < score) {
                                          max_scores[term] = score;
                                      }
                                  });
                              calc_max_status += 1;
                          });
        }
        group.wait();
        auto max_score = *std::max_element(max_scores.begin(), max_scores.end());
        std::cerr << fmt::format("Max partial score is: {}. It will be scaled to {}.",
                                 max_score,
                                 std::numeric_limits<std::uint8_t>::max());

        auto quantizer = [&](float score) {
            return static_cast<std::uint8_t>(score * std::numeric_limits<std::uint8_t>::max()
                                             / max_score);
        };
        std::vector<std::uint8_t> quantized_max_scores;
        std::transform(max_scores.begin(),
                       max_scores.end(),
                       std::back_inserter(quantized_max_scores),
                       quantizer);

        ProgressStatus status(
            index.num_terms(), DefaultProgress("Scoring"), std::chrono::milliseconds(100));
        std::ofstream score_file_stream(postings_path);
        auto offsets = score_index(index,
                                   score_file_stream,
                                   RawWriter<std::uint8_t>{},
                                   make_bm25(index),
                                   quantizer,
                                   [&]() { status += 1; });
        write_span(gsl::span<std::size_t const>(offsets), offsets_path);
        write_span(gsl::span<float const>(max_scores), max_scores_path);
        write_span(gsl::span<std::uint8_t const>(quantized_max_scores), quantized_max_scores_path);
    });
    meta.scores.push_back(PostingFilePaths{.postings = postings_path, .offsets = offsets_path});
    meta.max_scores["bm25"] = max_scores_path;
    meta.quantized_max_scores["bm25"] = quantized_max_scores_path;
    meta.update();
    return meta;
}

// TODO: Use multiple threads
auto bm_score_index(IndexMetadata meta, std::size_t block_size, std::size_t threads)
    -> IndexMetadata
{
    auto run = index_runner(meta);
    auto const& index_basename = meta.get_basename();
    auto prefix = fmt::format("{}.bm25_block_max", meta.get_basename());
    UnigramFilePaths paths{
        .documents = PostingFilePaths{.postings = fmt::format("{}_documents", prefix),
                                      .offsets = fmt::format("{}_document_offsets", prefix)},
        .payloads = PostingFilePaths{.postings = fmt::format("{}.scores", prefix),
                                     .offsets = fmt::format("{}.score_offsets", prefix)},
    };
    run([&](auto&& index) {
        auto scorer = make_bm25(index);
        ProgressStatus status(index.num_terms(),
                              DefaultProgress("Calculating max-blocks"),
                              std::chrono::milliseconds(100));
        std::ofstream document_out(paths.documents.postings);
        std::ofstream score_out(paths.payloads.postings);
        PostingBuilder<DocId> document_builder(RawWriter<DocId>{});
        PostingBuilder<float> score_builder(RawWriter<float>{});
        document_builder.write_header(document_out);
        score_builder.write_header(score_out);
        for (TermId term_id = 0; term_id < index.num_terms(); term_id += 1) {
            auto cursor = index.scored_cursor(term_id, scorer);
            while (not cursor.empty()) {
                auto max_score = 0.0F;
                auto last_docid = 0;
                for (auto idx = 0; idx < block_size && not cursor.empty(); ++idx) {
                    if (auto score = cursor.payload(); score > max_score) {
                        max_score = score;
                    }
                    last_docid = cursor.value();
                    cursor.advance();
                }
                document_builder.accumulate(last_docid);
                score_builder.accumulate(max_score);
            }
            document_builder.flush_segment(document_out);
            score_builder.flush_segment(score_out);
            status += 1;
        }
        write_span(gsl::make_span(document_builder.offsets()), paths.documents.offsets);
        write_span(gsl::make_span(score_builder.offsets()), paths.payloads.offsets);
    });
    meta.block_max_scores["bm25"] = paths;
    meta.update();
    return meta;
}

} // namespace pisa::v1
