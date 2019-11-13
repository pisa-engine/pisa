#include <string_view>

#include <CLI/CLI.hpp>
#include <spdlog/spdlog.h>

#include "binary_freq_collection.hpp"
#include "v1/blocked_cursor.hpp"
#include "v1/index_builder.hpp"
#include "v1/index_metadata.hpp"
#include "v1/progress_status.hpp"
#include "v1/raw_cursor.hpp"
#include "v1/types.hpp"

using pisa::v1::BlockedReader;
using pisa::v1::DefaultProgress;
using pisa::v1::IndexMetadata;
using pisa::v1::PostingFilePaths;
using pisa::v1::ProgressStatus;
using pisa::v1::RawReader;
using pisa::v1::RawWriter;
using pisa::v1::resolve_yml;
using pisa::v1::TermId;
using pisa::v1::write_span;

int main(int argc, char** argv)
{
    std::optional<std::string> yml{};
    int bytes_per_score = 1;
    std::size_t threads = std::thread::hardware_concurrency();

    CLI::App app{"Scores v1 index."};
    app.add_option("-i,--index",
                   yml,
                   "Path of .yml file of an index "
                   "(if not provided, it will be looked for in the current directory)",
                   false);
    app.add_option("-j,--threads", threads, "Number of threads");
    // TODO(michal): enable
    // app.add_option(
    //    "-b,--bytes-per-score", yml, "Quantize computed scores to this many bytes", true);
    CLI11_PARSE(app, argc, argv);

    auto resolved_yml = resolve_yml(yml);
    auto meta = IndexMetadata::from_file(resolved_yml);
    auto stemmer = meta.stemmer ? std::make_optional(*meta.stemmer) : std::optional<std::string>{};

    auto run = index_runner(meta,
                            RawReader<std::uint32_t>{},
                            BlockedReader<::pisa::simdbp_block, true>{},
                            BlockedReader<::pisa::simdbp_block, false>{});
    auto index_basename = resolved_yml.substr(0, resolved_yml.size() - 4);
    auto postings_path = fmt::format("{}.bm25", index_basename);
    auto offsets_path = fmt::format("{}.bm25_offsets", index_basename);
    run([&](auto&& index) {
        ProgressStatus calc_max_status(index.num_terms(),
                                       DefaultProgress("Calculating max partial score"),
                                       std::chrono::milliseconds(100));
        std::vector<float> max_scores(threads, 0.0F);
        tbb::task_group group;
        auto batch_size = index.num_terms() / threads;
        for (auto thread_id = 0; thread_id < threads; thread_id += 1) {
            auto first_term = thread_id * batch_size;
            auto end_term =
                thread_id < threads - 1 ? (thread_id + 1) * batch_size : index.num_terms();
            std::for_each(
                boost::counting_iterator<TermId>(first_term),
                boost::counting_iterator<TermId>(end_term),
                [&](auto term) {
                    for_each(index.scoring_cursor(term, make_bm25(index)), [&](auto& cursor) {
                        max_scores[thread_id] = std::max(max_scores[thread_id], cursor.payload());
                    });
                    calc_max_status += 1;
                });
        }
        group.wait();
        auto max_score = *std::max_element(max_scores.begin(), max_scores.end());
        std::cerr << fmt::format("Max partial score is: {}. It will be scaled to {}.",
                                 max_score,
                                 std::numeric_limits<std::uint8_t>::max());

        ProgressStatus status(
            index.num_terms(), DefaultProgress("Scoring"), std::chrono::milliseconds(100));
        std::ofstream score_file_stream(postings_path);
        auto offsets = score_index(
            index,
            score_file_stream,
            RawWriter<std::uint8_t>{},
            make_bm25(index),
            [&](float score) {
                return static_cast<std::uint8_t>(score * std::numeric_limits<std::uint8_t>::max()
                                                 / max_score);
            },
            [&]() { status += 1; });
        write_span(gsl::span<std::size_t const>(offsets), offsets_path);
    });
    meta.scores.push_back(PostingFilePaths{.postings = postings_path, .offsets = offsets_path});
    meta.write(resolved_yml);

    return 0;
}
