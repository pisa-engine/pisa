#include <tbb/task_group.h>

#include "codec/simdbp.hpp"
#include "v1/blocked_cursor.hpp"
#include "v1/index_builder.hpp"
#include "v1/index_metadata.hpp"
#include "v1/progress_status.hpp"
#include "v1/raw_cursor.hpp"
#include "v1/score_index.hpp"

using pisa::v1::BlockedReader;
using pisa::v1::DefaultProgress;
using pisa::v1::IndexMetadata;
using pisa::v1::PostingFilePaths;
using pisa::v1::ProgressStatus;
using pisa::v1::RawReader;
using pisa::v1::RawWriter;
using pisa::v1::TermId;
using pisa::v1::write_span;

namespace pisa::v1 {

void score_index(std::string const& yml, std::size_t threads)
{
    auto meta = IndexMetadata::from_file(yml);
    auto run = index_runner(meta,
                            RawReader<std::uint32_t>{},
                            BlockedReader<::pisa::simdbp_block, true>{},
                            BlockedReader<::pisa::simdbp_block, false>{});
    auto index_basename = yml.substr(0, yml.size() - 4);
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
                                  index.scoring_cursor(term, make_bm25(index)), [&](auto& cursor) {
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
    meta.write(yml);
}

} // namespace pisa::v1
