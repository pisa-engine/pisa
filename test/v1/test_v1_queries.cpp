#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <string>
#include <tuple>

#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>
#include <tbb/task_scheduler_init.h>

#include "../temporary_directory.hpp"
#include "accumulator/lazy_accumulator.hpp"
#include "cursor/block_max_scored_cursor.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/scored_cursor.hpp"
#include "index_fixture.hpp"
#include "index_types.hpp"
#include "io.hpp"
#include "pisa_config.hpp"
#include "query/algorithm/ranked_or_query.hpp"
#include "query/queries.hpp"
#include "scorer/bm25.hpp"
#include "v1/bit_sequence_cursor.hpp"
#include "v1/blocked_cursor.hpp"
#include "v1/cursor/collect.hpp"
#include "v1/cursor_intersection.hpp"
#include "v1/cursor_traits.hpp"
#include "v1/cursor_union.hpp"
#include "v1/daat_and.hpp"
#include "v1/daat_or.hpp"
#include "v1/index.hpp"
#include "v1/index_builder.hpp"
#include "v1/maxscore.hpp"
#include "v1/posting_builder.hpp"
#include "v1/posting_format_header.hpp"
#include "v1/query.hpp"
#include "v1/score_index.hpp"
#include "v1/scorer/bm25.hpp"
#include "v1/sequence/partitioned_sequence.hpp"
#include "v1/sequence/positive_sequence.hpp"
#include "v1/taat_or.hpp"
#include "v1/types.hpp"
#include "v1/union_lookup.hpp"
#include "v1/wand.hpp"

using pisa::v1::DocId;
using pisa::v1::DocumentBitSequenceCursor;
using pisa::v1::DocumentBlockedCursor;
using pisa::v1::Frequency;
using pisa::v1::Index;
using pisa::v1::IndexMetadata;
using pisa::v1::ListSelection;
using pisa::v1::PartitionedSequence;
using pisa::v1::PayloadBitSequenceCursor;
using pisa::v1::PayloadBlockedCursor;
using pisa::v1::PositiveSequence;
using pisa::v1::RawCursor;

static constexpr auto RELATIVE_ERROR = 0.1F;

template <typename v0_Index, typename v1_Index, typename ScoredIndex>
struct IndexData {

    static std::unique_ptr<IndexData> data;

    IndexData()
        : collection(PISA_SOURCE_DIR "/test/test_data/test_collection"),
          document_sizes(PISA_SOURCE_DIR "/test/test_data/test_collection.sizes"),
          wdata(document_sizes.begin()->begin(),
                collection.num_docs(),
                collection,
                "bm25",
                ::pisa::BlockSize(::pisa::FixedBlock()),
                {})

    {
        typename v0_Index::builder builder(collection.num_docs(), params);
        for (auto const& plist : collection) {
            uint64_t freqs_sum =
                std::accumulate(plist.freqs.begin(), plist.freqs.end(), uint64_t(0));
            builder.add_posting_list(
                plist.docs.size(), plist.docs.begin(), plist.freqs.begin(), freqs_sum);
        }
        builder.build(v0_index);

        ::pisa::term_id_vec q;
        std::ifstream qfile(PISA_SOURCE_DIR "/test/test_data/queries");
        auto push_query = [&](std::string const& query_line) {
            queries.push_back(::pisa::parse_query_ids(query_line));
        };
        ::pisa::io::for_each_line(qfile, push_query);

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

    ::pisa::global_parameters params;
    ::pisa::binary_freq_collection collection;
    ::pisa::binary_collection document_sizes;
    v0_Index v0_index;
    std::vector<::pisa::Query> queries;
    std::vector<float> thresholds;
    ::pisa::wand_data<::pisa::wand_data_raw> wdata;
};

/// Inefficient, do not use in production code.
[[nodiscard]] auto approximate_order(std::pair<float, std::uint32_t> lhs,
                                     std::pair<float, std::uint32_t> rhs) -> bool
{
    return std::make_pair(fmt::format("{:0.4f}", lhs.first), lhs.second)
           < std::make_pair(fmt::format("{:0.4f}", rhs.first), rhs.second);
}

/// Inefficient, do not use in production code.
[[nodiscard]] auto approximate_order_f(float lhs, float rhs) -> bool
{
    return fmt::format("{:0.4f}", lhs) < fmt::format("{:0.4f}", rhs);
}

template <typename v0_Index, typename v1_Index, typename ScoredIndex>
std::unique_ptr<IndexData<v0_Index, v1_Index, ScoredIndex>>
    IndexData<v0_Index, v1_Index, ScoredIndex>::data = nullptr;

TEMPLATE_TEST_CASE("Query",
                   "[v1][integration]",
                   (IndexFixture<RawCursor<DocId>, RawCursor<Frequency>, RawCursor<std::uint8_t>>),
                   (IndexFixture<DocumentBlockedCursor<::pisa::simdbp_block>,
                                 PayloadBlockedCursor<::pisa::simdbp_block>,
                                 RawCursor<std::uint8_t>>),
                   (IndexFixture<DocumentBitSequenceCursor<PartitionedSequence<>>,
                                 PayloadBitSequenceCursor<PositiveSequence<>>,
                                 RawCursor<std::uint8_t>>))
{
    tbb::task_scheduler_init init(1);
    auto data = IndexData<::pisa::single_index,
                          Index<RawCursor<DocId>, RawCursor<Frequency>>,
                          Index<RawCursor<DocId>, RawCursor<float>>>::get();
    TestType fixture;
    auto input_data = GENERATE(table<char const*, bool, bool>({
        //{"daat_or", false, false},
        //{"maxscore", false, false},
        //{"maxscore", true, false},
        //{"wand", false, false},
        //{"wand", true, false},
        //{"bmw", false, false},
        //{"bmw", true, false},
        //{"bmw", false, true},
        //{"bmw", true, true},
        //{"maxscore_union_lookup", true, false},
        //{"unigram_union_lookup", true, false},
        //{"union_lookup", true, false},
        {"union_lookup_plus", true, false},
        {"lookup_union", true, false},
    }));
    std::string algorithm = std::get<0>(input_data);
    bool with_threshold = std::get<1>(input_data);
    bool rebuild_with_variable_blocks = std::get<2>(input_data);
    if (rebuild_with_variable_blocks) {
        fixture.rebuild_bm_scores(pisa::v1::VariableBlock{12.0});
    }
    CAPTURE(algorithm);
    CAPTURE(with_threshold);
    auto index_basename = (fixture.tmpdir().path() / "inv").string();
    auto meta = IndexMetadata::from_file(fmt::format("{}.yml", index_basename));
    auto heap = ::pisa::topk_queue(10);
    ::pisa::ranked_or_query or_q(heap);
    auto run_query = [](std::string const& name, auto query, auto&& index, auto scorer) {
        if (name == "daat_or") {
            return daat_or(query, index, ::pisa::topk_queue(10), scorer);
        }
        if (name == "maxscore") {
            return maxscore(query, index, ::pisa::topk_queue(10), scorer);
        }
        if (name == "wand") {
            return wand(query, index, ::pisa::topk_queue(10), scorer);
        }
        if (name == "bmw") {
            return bmw(query, index, ::pisa::topk_queue(10), scorer);
        }
        if (name == "maxscore_union_lookup") {
            return maxscore_union_lookup(query, index, ::pisa::topk_queue(10), scorer);
        }
        if (name == "unigram_union_lookup") {
            query.selections(ListSelection{.unigrams = query.get_term_ids(), .bigrams = {}});
            return unigram_union_lookup(query, index, ::pisa::topk_queue(10), scorer);
        }
        if (name == "union_lookup") {
            if (query.get_term_ids().size() > 8) {
                return maxscore_union_lookup(query, index, ::pisa::topk_queue(10), scorer);
            }
            return union_lookup(query, index, ::pisa::topk_queue(10), scorer);
        }
        if (name == "union_lookup_plus") {
            if (query.get_term_ids().size() > 8) {
                return maxscore_union_lookup(query, index, ::pisa::topk_queue(10), scorer);
            }
            return union_lookup_plus(query, index, ::pisa::topk_queue(10), scorer);
        }
        if (name == "lookup_union") {
            return lookup_union(query, index, ::pisa::topk_queue(10), scorer);
        }
        std::abort();
    };
    int idx = 0;
    auto const intersections =
        pisa::v1::read_intersections(PISA_SOURCE_DIR "/test/test_data/top10_selections");
    for (auto& query : test_queries()) {
        heap.clear();
        if (algorithm == "union_lookup" || algorithm == "union_lookup_plus"
            || algorithm == "lookup_union") {
            query.selections(gsl::make_span(intersections[idx]));
        }

        CAPTURE(query);
        CAPTURE(idx);
        CAPTURE(intersections[idx]);

        or_q(
            make_scored_cursors(data->v0_index,
                                ::pisa::bm25<::pisa::wand_data<::pisa::wand_data_raw>>(data->wdata),
                                ::pisa::Query{{}, query.get_term_ids(), {}}),
            data->v0_index.num_docs());
        heap.finalize();
        auto expected = or_q.topk();
        if (with_threshold) {
            query.threshold(expected.back().first - 1.0F);
        }

        auto on_the_fly = [&]() {
            auto run = pisa::v1::index_runner(meta,
                                              std::make_tuple(fixture.document_reader()),
                                              std::make_tuple(fixture.frequency_reader()));
            std::vector<typename ::pisa::topk_queue::entry_type> results;
            run([&](auto&& index) {
                auto que = run_query(algorithm, query, index, make_bm25(index));
                que.finalize();
                results = que.topk();
                if (not results.empty()) {
                    results.erase(std::remove_if(results.begin(),
                                                 results.end(),
                                                 [last_score = results.back().first](auto&& entry) {
                                                     return entry.first <= last_score;
                                                 }),
                                  results.end());
                    std::sort(results.begin(), results.end(), approximate_order);
                }
            });
            return results;
        }();
        expected.resize(on_the_fly.size());
        std::sort(expected.begin(), expected.end(), approximate_order);

        // if (algorithm == "bmw") {
        //    for (size_t i = 0; i < on_the_fly.size(); ++i) {
        //        std::cerr << fmt::format("{} {} -- {} {}\n",
        //                                 on_the_fly[i].second,
        //                                 on_the_fly[i].first,
        //                                 expected[i].second,
        //                                 expected[i].first);
        //    }
        //    std::cerr << '\n';
        //}

        for (size_t i = 0; i < on_the_fly.size(); ++i) {
            REQUIRE(on_the_fly[i].second == expected[i].second);
            REQUIRE(on_the_fly[i].first == Approx(expected[i].first).epsilon(RELATIVE_ERROR));
        }

        idx += 1;

        if (algorithm == "bmw") {
            continue;
        }

        auto precomputed = [&]() {
            auto run = pisa::v1::scored_index_runner(meta,
                                                     std::make_tuple(fixture.document_reader()),
                                                     std::make_tuple(fixture.score_reader()));
            std::vector<typename ::pisa::topk_queue::entry_type> results;
            run([&](auto&& index) {
                auto que = run_query(algorithm, query, index, v1::VoidScorer{});
                que.finalize();
                results = que.topk();
            });
            if (not results.empty()) {
                results.erase(std::remove_if(results.begin(),
                                             results.end(),
                                             [last_score = results.back().first](auto&& entry) {
                                                 return entry.first <= last_score;
                                             }),
                              results.end());
                std::sort(results.begin(), results.end(), approximate_order);
            }
            return results;
        }();

        // TODO(michal): test the quantized results
        // constexpr float max_partial_score = 16.5724F;
        // auto quantizer = [&](float score) {
        //    return static_cast<std::uint8_t>(score * std::numeric_limits<std::uint8_t>::max()
        //                                     / max_partial_score);
        //};
    }
}
