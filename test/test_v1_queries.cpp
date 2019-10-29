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
#include "v1/cursor/collect.hpp"
#include "v1/cursor_intersection.hpp"
#include "v1/cursor_union.hpp"
#include "v1/index.hpp"
#include "v1/posting_builder.hpp"
#include "v1/posting_format_header.hpp"
#include "v1/scorer/bm25.hpp"
#include "v1/types.hpp"

namespace v1 = pisa::v1;
using namespace pisa;

template <typename v0_Index, typename v1_Index>
struct IndexData {

    static std::unique_ptr<IndexData> data;

    IndexData()
        : collection(PISA_SOURCE_DIR "/test/test_data/test_collection"),
          document_sizes(PISA_SOURCE_DIR "/test/test_data/test_collection.sizes"),
          v1_index(
              pisa::v1::binary_collection_index(PISA_SOURCE_DIR "/test/test_data/test_collection")),
          wdata(document_sizes.begin()->begin(),
                collection.num_docs(),
                collection,
                BlockSize(FixedBlock()))

    {
        tbb::task_scheduler_init init;
        typename v0_Index::builder builder(collection.num_docs(), params);
        for (auto const &plist : collection) {
            uint64_t freqs_sum =
                std::accumulate(plist.freqs.begin(), plist.freqs.end(), uint64_t(0));
            builder.add_posting_list(
                plist.docs.size(), plist.docs.begin(), plist.freqs.begin(), freqs_sum);
        }
        builder.build(v0_index);

        term_id_vec q;
        std::ifstream qfile(PISA_SOURCE_DIR "/test/test_data/queries");
        auto push_query = [&](std::string const &query_line) {
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
            IndexData::data = std::make_unique<IndexData<v0_Index, v1_Index>>();
        }
        return IndexData::data.get();
    }

    global_parameters params;
    binary_freq_collection collection;
    binary_collection document_sizes;
    v0_Index v0_index;
    v1_Index v1_index;
    std::vector<Query> queries;
    std::vector<float> thresholds;
    wand_data<wand_data_raw> wdata;
};

template <typename v0_Index, typename v1_Index>
std::unique_ptr<IndexData<v0_Index, v1_Index>> IndexData<v0_Index, v1_Index>::data = nullptr;

template <typename Index>
auto daat_and(Query const &query, Index const &index, topk_queue topk)
{
    v1::BM25 scorer(index);
    std::vector<decltype(index.scoring_cursor(0, scorer))> cursors;
    std::transform(query.terms.begin(),
                   query.terms.end(),
                   std::back_inserter(cursors),
                   [&](auto term) { return index.scoring_cursor(term, scorer); });
    auto intersection =
        v1::intersect(std::move(cursors), 0.0F, [](auto &score, auto &cursor, auto /* term_idx */) {
            score += cursor.payload();
            return score;
        });
    v1::for_each(intersection, [&](auto &cursor) { topk.insert(cursor.payload(), *cursor); });
    return topk;
}

template <typename Index>
auto daat_or(Query const &query, Index const &index, topk_queue topk)
{
    v1::BM25 scorer(index);
    std::vector<decltype(index.scoring_cursor(0, scorer))> cursors;
    std::transform(query.terms.begin(),
                   query.terms.end(),
                   std::back_inserter(cursors),
                   [&](auto term) { return index.scoring_cursor(term, scorer); });
    auto cunion = v1::union_merge(
        std::move(cursors), 0.0F, [](auto &score, auto &cursor, auto /* term_idx */) {
            score += cursor.payload();
            return score;
        });
    v1::for_each(cunion, [&](auto &cursor) { topk.insert(cursor.payload(), *cursor); });
    return topk;
}

TEST_CASE("DAAT AND", "[v1][integration]")
{
    auto data = IndexData<single_index,
                          v1::Index<v1::RawCursor<v1::DocId>, v1::RawCursor<v1::Frequency>>>::get();
    ranked_and_query and_q(10);
    int idx = 0;
    for (auto const &q : data->queries) {
        CAPTURE(q.terms);
        CAPTURE(idx++);
        and_q(make_scored_cursors(data->v0_index, data->wdata, q), data->v0_index.num_docs());
        auto que = daat_and(q, data->v1_index, topk_queue(10));
        que.finalize();

        auto expected = and_q.topk();
        std::sort(expected.begin(), expected.end(), std::greater{});
        auto actual = que.topk();
        std::sort(actual.begin(), actual.end(), std::greater{});

        REQUIRE(expected.size() == actual.size());
        for (size_t i = 0; i < actual.size(); ++i) {
            REQUIRE(actual[i].second == expected[i].second);
            REQUIRE(actual[i].first == Approx(expected[i].first).epsilon(0.1));
        }
    }
}

TEST_CASE("DAAT OR", "[v1][integration]")
{
    auto data = IndexData<single_index,
                          v1::Index<v1::RawCursor<v1::DocId>, v1::RawCursor<v1::Frequency>>>::get();
    ranked_or_query or_q(10);
    int idx = 0;
    for (auto const &q : data->queries) {
        CAPTURE(q.terms);
        CAPTURE(idx++);
        or_q(make_scored_cursors(data->v0_index, data->wdata, q), data->v0_index.num_docs());
        auto que = daat_or(q, data->v1_index, topk_queue(10));
        que.finalize();

        auto expected = or_q.topk();
        std::sort(expected.begin(), expected.end(), std::greater{});
        auto actual = que.topk();
        std::sort(actual.begin(), actual.end(), std::greater{});

        REQUIRE(expected.size() == actual.size());
        for (size_t i = 0; i < actual.size(); ++i) {
            REQUIRE(actual[i].second == expected[i].second);
            REQUIRE(actual[i].first == Approx(expected[i].first).epsilon(0.1));
        }
    }
}
