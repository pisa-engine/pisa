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
using pisa::v1::resolve_ini;
using pisa::v1::write_span;

int main(int argc, char **argv)
{
    std::optional<std::string> ini{};

    CLI::App app{"Scores v1 index."};
    app.add_option("-i,--index",
                   ini,
                   "Path of .ini file of an index "
                   "(if not provided, it will be looked for in the current directory)",
                   false);
    CLI11_PARSE(app, argc, argv);

    auto resolved_ini = resolve_ini(ini);
    auto meta = IndexMetadata::from_file(resolved_ini);
    auto stemmer = meta.stemmer ? std::make_optional(*meta.stemmer) : std::optional<std::string>{};

    auto run = index_runner(meta,
                            RawReader<std::uint32_t>{},
                            BlockedReader<::pisa::simdbp_block, true>{},
                            BlockedReader<::pisa::simdbp_block, false>{});
    auto index_basename = resolved_ini.substr(0, resolved_ini.size() - 4);
    auto postings_path = fmt::format("{}.bm25", index_basename);
    auto offsets_path = fmt::format("{}.bm25_offsets", index_basename);
    run([&](auto &&index) {
        ProgressStatus status(
            index.num_terms(), DefaultProgress("Scoring"), std::chrono::milliseconds(100));
        std::ofstream score_file_stream(postings_path);
        auto offsets = score_index(
            index, score_file_stream, RawWriter<float>{}, make_bm25(index), [&]() { status += 1; });
        write_span(gsl::span<std::size_t const>(offsets), offsets_path);
    });
    meta.scores.push_back(PostingFilePaths{.postings = postings_path, .offsets = offsets_path});
    meta.write(resolved_ini);

    return 0;
}
