#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <string>

#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>
#include <tbb/task_scheduler_init.h>

#include "accumulator/lazy_accumulator.hpp"
#include "cursor/block_max_scored_cursor.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/scored_cursor.hpp"
#include "index_types.hpp"
#include "io.hpp"
#include "pisa_config.hpp"
#include "query/queries.hpp"
#include "temporary_directory.hpp"
#include "v1/blocked_cursor.hpp"
#include "v1/cursor/collect.hpp"
#include "v1/cursor_intersection.hpp"
#include "v1/cursor_traits.hpp"
#include "v1/cursor_union.hpp"
#include "v1/index.hpp"
#include "v1/index_builder.hpp"
#include "v1/posting_builder.hpp"
#include "v1/posting_format_header.hpp"
#include "v1/query.hpp"
#include "v1/scorer/bm25.hpp"
#include "v1/types.hpp"

namespace v1 = pisa::v1;
using namespace pisa;

static constexpr auto RELATIVE_ERROR = 0.1F;

template <typename DocumentCursor, typename FrequencyCursor, typename ScoreCursor>
struct IndexFixture {
    using DocumentWriter = typename v1::CursorTraits<DocumentCursor>::Writer;
    using FrequencyWriter = typename v1::CursorTraits<FrequencyCursor>::Writer;
    using ScoreWriter = typename v1::CursorTraits<ScoreCursor>::Writer;

    using DocumentReader = typename v1::CursorTraits<DocumentCursor>::Reader;
    using FrequencyReader = typename v1::CursorTraits<FrequencyCursor>::Reader;
    using ScoreReader = typename v1::CursorTraits<ScoreCursor>::Reader;

    IndexFixture() : m_tmpdir(std::make_unique<Temporary_Directory>())
    {
        auto index_basename = (tmpdir().path() / "inv").string();
        v1::compress_binary_collection(PISA_SOURCE_DIR "/test/test_data/test_collection",
                                       PISA_SOURCE_DIR "/test/test_data/test_collection.fwd",
                                       index_basename,
                                       2,
                                       v1::make_writer<DocumentWriter>(),
                                       v1::make_writer<FrequencyWriter>());
        auto errors = v1::verify_compressed_index(PISA_SOURCE_DIR "/test/test_data/test_collection",
                                                  index_basename);
        REQUIRE(errors.empty());
        auto meta = v1::IndexMetadata::from_file(fmt::format("{}.yml", index_basename));
        auto run = v1::index_runner(meta, document_reader(), frequency_reader());
        auto postings_path = fmt::format("{}.bm25", index_basename);
        auto offsets_path = fmt::format("{}.bm25_offsets", index_basename);
        run([&](auto&& index) {
            std::ofstream score_file_stream(postings_path);
            auto offsets = score_index(index, score_file_stream, ScoreWriter{}, make_bm25(index));
            v1::write_span(gsl::span<std::size_t const>(offsets), offsets_path);
        });
        meta.scores.push_back(
            v1::PostingFilePaths{.postings = postings_path, .offsets = offsets_path});
        meta.write(fmt::format("{}.yml", index_basename));
    }

    [[nodiscard]] auto const& tmpdir() const { return *m_tmpdir; }
    [[nodiscard]] auto document_reader() const { return m_document_reader; }
    [[nodiscard]] auto frequency_reader() const { return m_frequency_reader; }
    [[nodiscard]] auto score_reader() const { return m_score_reader; }

   private:
    std::unique_ptr<Temporary_Directory const> m_tmpdir;
    DocumentReader m_document_reader{};
    FrequencyReader m_frequency_reader{};
    ScoreReader m_score_reader{};
};

[[nodiscard]] auto test_queries() -> std::vector<Query>
{
    std::vector<Query> queries;
    std::ifstream qfile(PISA_SOURCE_DIR "/test/test_data/queries");
    auto push_query = [&](std::string const& query_line) {
        queries.push_back(parse_query_ids(query_line));
    };
    io::for_each_line(qfile, push_query);
    return queries;
}

template <typename v0_Index, typename v1_Index, typename ScoredIndex>
struct IndexData {

    static std::unique_ptr<IndexData> data;

    IndexData()
        : collection(PISA_SOURCE_DIR "/test/test_data/test_collection"),
          document_sizes(PISA_SOURCE_DIR "/test/test_data/test_collection.sizes"),
          v1_index(
              pisa::v1::binary_collection_index(PISA_SOURCE_DIR "/test/test_data/test_collection")),
          scored_index(pisa::v1::binary_collection_scored_index(PISA_SOURCE_DIR
                                                                "/test/test_data/test_collection")),
          wdata(document_sizes.begin()->begin(),
                collection.num_docs(),
                collection,
                BlockSize(FixedBlock()))

    {
        typename v0_Index::builder builder(collection.num_docs(), params);
        for (auto const& plist : collection) {
            uint64_t freqs_sum =
                std::accumulate(plist.freqs.begin(), plist.freqs.end(), uint64_t(0));
            builder.add_posting_list(
                plist.docs.size(), plist.docs.begin(), plist.freqs.begin(), freqs_sum);
        }
        builder.build(v0_index);

        term_id_vec q;
        std::ifstream qfile(PISA_SOURCE_DIR "/test/test_data/queries");
        auto push_query = [&](std::string const& query_line) {
            queries.push_back(parse_query_ids(query_line));
        };
        io::for_each_line(qfile, push_query);

        std::string t;
        std::ifstream tin(PISA_SOURCE_DIR "/test/test_data/top5_thresholds");
        while (std::getline(tin, t)) {
            thresholds.push_back(std::stof(t));
        }
    }

    [[nodiscard]] static auto get()
    {
        if (IndexData::data == nullptr) {
            IndexData::data = std::make_unique<IndexData<v0_Index, v1_Index, ScoredIndex>>();
        }
        return IndexData::data.get();
    }

    global_parameters params;
    binary_freq_collection collection;
    binary_collection document_sizes;
    v0_Index v0_index;
    v1_Index v1_index;
    ScoredIndex scored_index;
    std::vector<Query> queries;
    std::vector<float> thresholds;
    wand_data<wand_data_raw> wdata;
};

template <typename v0_Index, typename v1_Index, typename ScoredIndex>
std::unique_ptr<IndexData<v0_Index, v1_Index, ScoredIndex>>
    IndexData<v0_Index, v1_Index, ScoredIndex>::data = nullptr;

TEMPLATE_TEST_CASE(
    "DAAT OR",
    "[v1][integration]",
    (IndexFixture<v1::RawCursor<v1::DocId>, v1::RawCursor<v1::Frequency>, v1::RawCursor<float>>),
    (IndexFixture<v1::BlockedCursor<::pisa::simdbp_block, true>,
                  v1::BlockedCursor<::pisa::simdbp_block, false>,
                  v1::RawCursor<float>>))
{
    tbb::task_scheduler_init init(1);
    auto data = IndexData<single_index,
                          v1::Index<v1::RawCursor<v1::DocId>, v1::RawCursor<v1::Frequency>>,
                          v1::Index<v1::RawCursor<v1::DocId>, v1::RawCursor<float>>>::get();
    TestType fixture;
    auto index_basename = (fixture.tmpdir().path() / "inv").string();
    auto meta = v1::IndexMetadata::from_file(fmt::format("{}.yml", index_basename));
    ranked_or_query or_q(10);
    int idx = 0;
    for (auto const& q : test_queries()) {
        CAPTURE(q.terms);
        CAPTURE(idx++);

        or_q(make_scored_cursors(data->v0_index, data->wdata, q), data->v0_index.num_docs());
        auto expected = or_q.topk();
        std::sort(expected.begin(), expected.end(), std::greater{});

        auto on_the_fly = [&]() {
            auto run =
                v1::index_runner(meta, fixture.document_reader(), fixture.frequency_reader());
            std::vector<typename topk_queue::entry_type> results;
            run([&](auto&& index) {
                auto que = daat_or(v1::Query{q.terms}, index, topk_queue(10), make_bm25(index));
                que.finalize();
                results = que.topk();
                std::sort(results.begin(), results.end(), std::greater{});
            });
            return results;
        }();

        auto precomputed = [&]() {
            auto run =
                v1::scored_index_runner(meta, fixture.document_reader(), fixture.score_reader());
            std::vector<typename topk_queue::entry_type> results;
            run([&](auto&& index) {
                auto que = daat_or(v1::Query{q.terms}, index, topk_queue(10), v1::VoidScorer{});
                que.finalize();
                results = que.topk();
                std::sort(results.begin(), results.end(), std::greater{});
            });
            return results;
        }();

        REQUIRE(expected.size() == on_the_fly.size());
        REQUIRE(expected.size() == precomputed.size());
        for (size_t i = 0; i < on_the_fly.size(); ++i) {
            REQUIRE(on_the_fly[i].second == expected[i].second);
            REQUIRE(on_the_fly[i].first == Approx(expected[i].first).epsilon(RELATIVE_ERROR));
            REQUIRE(precomputed[i].second == expected[i].second);
            REQUIRE(precomputed[i].first == Approx(expected[i].first).epsilon(RELATIVE_ERROR));
        }
    }
}

TEMPLATE_TEST_CASE(
    "UnionLookup",
    "[v1][integration]",
    (IndexFixture<v1::RawCursor<v1::DocId>, v1::RawCursor<v1::Frequency>, v1::RawCursor<float>>),
    (IndexFixture<v1::BlockedCursor<::pisa::simdbp_block, true>,
                  v1::BlockedCursor<::pisa::simdbp_block, false>,
                  v1::RawCursor<float>>))
{
    tbb::task_scheduler_init init(1);
    auto data = IndexData<single_index,
                          v1::Index<v1::RawCursor<v1::DocId>, v1::RawCursor<v1::Frequency>>,
                          v1::Index<v1::RawCursor<v1::DocId>, v1::RawCursor<float>>>::get();
    TestType fixture;
    auto index_basename = (fixture.tmpdir().path() / "inv").string();
    auto meta = v1::IndexMetadata::from_file(fmt::format("{}.yml", index_basename));
    ranked_or_query or_q(10);
    int idx = 0;
    for (auto& q : test_queries()) {
        CAPTURE(q.terms);
        CAPTURE(idx++);

        or_q(make_scored_cursors(data->v0_index, data->wdata, q), data->v0_index.num_docs());
        auto expected = or_q.topk();
        std::sort(expected.begin(), expected.end(), std::greater{});

        auto on_the_fly = [&]() {
            auto run =
                v1::index_runner(meta, fixture.document_reader(), fixture.frequency_reader());
            std::vector<typename topk_queue::entry_type> results;
            run([&](auto&& index) {
                std::vector<std::size_t> unigrams(q.terms.size());
                std::iota(unigrams.begin(), unigrams.end(), 0);
                auto que = union_lookup(v1::Query{q.terms},
                                        index,
                                        topk_queue(10),
                                        make_bm25(index),
                                        std::move(unigrams),
                                        {});
                que.finalize();
                results = que.topk();
                std::sort(results.begin(), results.end(), std::greater{});
            });
            return results;
        }();

        // auto precomputed = [&]() {
        //    auto run =
        //        v1::scored_index_runner(meta, fixture.document_reader(), fixture.score_reader());
        //    std::vector<typename topk_queue::entry_type> results;
        //    run([&](auto&& index) {
        //        auto que = daat_or(v1::Query{q.terms}, index, topk_queue(10), v1::VoidScorer{});
        //        que.finalize();
        //        results = que.topk();
        //        std::sort(results.begin(), results.end(), std::greater{});
        //    });
        //    return results;
        //}();

        REQUIRE(expected.size() == on_the_fly.size());
        // REQUIRE(expected.size() == precomputed.size());
        for (size_t i = 0; i < on_the_fly.size(); ++i) {
            REQUIRE(on_the_fly[i].second == expected[i].second);
            REQUIRE(on_the_fly[i].first == Approx(expected[i].first).epsilon(RELATIVE_ERROR));
            // REQUIRE(precomputed[i].second == expected[i].second);
            // REQUIRE(precomputed[i].first == Approx(expected[i].first).epsilon(RELATIVE_ERROR));
        }
    }
}
