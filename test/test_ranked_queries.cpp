#include "query/query_parser.hpp"
#include "type_safe.hpp"
#define CATCH_CONFIG_MAIN

#include <memory>

#include <catch2/catch.hpp>

#include "accumulator/lazy_accumulator.hpp"
#include "accumulator/simple_accumulator.hpp"
#include "cursor/block_max_scored_cursor.hpp"
#include "cursor/scored_cursor.hpp"
#include "index_types.hpp"
#include "io.hpp"
#include "pisa_config.hpp"
#include "query/algorithm/block_max_maxscore_query.hpp"
#include "query/algorithm/block_max_ranked_and_query.hpp"
#include "query/algorithm/block_max_wand_query.hpp"
#include "query/algorithm/maxscore_query.hpp"
#include "query/algorithm/range_query.hpp"
#include "query/algorithm/ranked_and_query.hpp"
#include "query/algorithm/ranked_or_query.hpp"
#include "query/algorithm/ranked_or_taat_query.hpp"
#include "query/algorithm/wand_query.hpp"
#include "scorer/scorer.hpp"
#include "wand_data.hpp"
#include "wand_data_raw.hpp"
#include "wand_utils.hpp"

using namespace pisa;

template <typename Index>
struct IndexData {
    static std::unordered_map<std::string, std::unique_ptr<IndexData>> data;

    IndexData(std::string const& scorer_name, bool quantized, std::unordered_set<size_t> const& dropped_term_ids)
        : collection(PISA_SOURCE_DIR "/test/test_data/test_collection"),
          document_sizes(PISA_SOURCE_DIR "/test/test_data/test_collection.sizes"),
          wdata(
              document_sizes.begin()->begin(),
              collection.num_docs(),
              collection,
              ScorerParams(scorer_name),
              BlockSize(FixedBlock(5)),
              quantized ? std::optional<Size>(Size(8)) : std::nullopt,
              dropped_term_ids
          )

    {
        typename Index::builder builder(collection.num_docs(), params);
        for (auto const& plist: collection) {
            uint64_t freqs_sum = std::accumulate(plist.freqs.begin(), plist.freqs.end(), uint64_t(0));
            builder.add_posting_list(
                plist.docs.size(), plist.docs.begin(), plist.freqs.begin(), freqs_sum
            );
        }
        builder.build(index);

        std::vector<TermId> q;
        QueryParser parser(
            TextAnalyzer(std::make_unique<WhitespaceTokenizer>()), std::make_unique<IntMap>()
        );
        std::ifstream qfile(PISA_SOURCE_DIR "/test/test_data/queries");
        auto push_query = [&](std::string const& query_line) {
            queries.push_back(parser.parse(query_line));
        };
        io::for_each_line(qfile, push_query);

        std::string t;
    }

    [[nodiscard]] static auto
    get(std::string const& s_name, bool quantized, std::unordered_set<size_t> const& dropped_term_ids
    ) {
        if (IndexData::data.find(s_name) == IndexData::data.end()) {
            IndexData::data[s_name] =
                std::make_unique<IndexData<Index>>(s_name, quantized, dropped_term_ids);
        }
        return IndexData::data[s_name].get();
    }

    global_parameters params;
    binary_freq_collection collection;
    binary_collection document_sizes;
    Index index;
    std::vector<Query> queries;
    wand_data<wand_data_raw> wdata;
};

template <typename Index>
std::unordered_map<std::string, std::unique_ptr<IndexData<Index>>> IndexData<Index>::data = {};

template <typename Acc>
class ranked_or_taat_query_acc: public ranked_or_taat_query {
  public:
    using ranked_or_taat_query::ranked_or_taat_query;

    template <typename CursorRange>
    PISA_REQUIRES((pisa::concepts::MaxScorePostingCursor<typename std::decay_t<CursorRange>::value_type>))
    void operator()(CursorRange&& cursors, uint64_t max_docid) {
        Acc accumulator(max_docid);
        ranked_or_taat_query::operator()(cursors, max_docid, accumulator);
    }
};

template <typename T>
class range_query_128: public range_query<T> {
  public:
    using range_query<T>::range_query;

    template <typename CursorRange>
    PISA_REQUIRES((pisa::concepts::MaxScorePostingCursor<typename std::decay_t<CursorRange>::value_type>))
    void operator()(CursorRange&& cursors, uint64_t max_docid) {
        range_query<T>::operator()(cursors, max_docid, 128);
    }
};

// NOLINTNEXTLINE(hicpp-explicit-conversions)
TEMPLATE_TEST_CASE("Ranked query test", "[query][ranked][integration]", ranked_or_taat_query_acc<SimpleAccumulator>, ranked_or_taat_query_acc<LazyAccumulator<4>>, wand_query, maxscore_query, block_max_wand_query, block_max_maxscore_query, range_query_128<ranked_or_taat_query_acc<SimpleAccumulator>>, range_query_128<ranked_or_taat_query_acc<LazyAccumulator<4>>>, range_query_128<wand_query>, range_query_128<maxscore_query>, range_query_128<block_max_wand_query>, range_query_128<block_max_maxscore_query>) {
    for (auto quantized: {false, true}) {
        for (auto&& s_name: {"bm25", "qld"}) {
            std::unordered_set<size_t> dropped_term_ids;
            auto data = IndexData<single_index>::get(s_name, quantized, dropped_term_ids);
            topk_queue topk_1(10);
            TestType op_q(topk_1);
            topk_queue topk_2(10);
            ranked_or_query or_q(topk_2);

            auto scorer = scorer::from_params(ScorerParams(s_name), data->wdata);
            for (auto const& q: data->queries) {
                or_q(make_scored_cursors(data->index, *scorer, q), data->index.num_docs());
                op_q(
                    make_block_max_scored_cursors(data->index, data->wdata, *scorer, q),
                    data->index.num_docs()
                );
                topk_1.finalize();
                topk_2.finalize();
                REQUIRE(topk_2.topk().size() == topk_1.topk().size());
                for (size_t i = 0; i < topk_2.topk().size(); ++i) {
                    REQUIRE(
                        topk_2.topk()[i].first == Approx(topk_1.topk()[i].first).epsilon(0.1)
                    );  // tolerance is %
                        // relative
                }
                topk_1.clear();
                topk_2.clear();
            }
        }
    }
}

// NOLINTNEXTLINE(hicpp-explicit-conversions)
TEMPLATE_TEST_CASE("Ranked AND query test", "[query][ranked][integration]", block_max_ranked_and_query) {
    for (auto quantized: {false, true}) {
        for (auto&& s_name: {"bm25", "qld"}) {
            std::unordered_set<size_t> dropped_term_ids;
            auto data = IndexData<single_index>::get(s_name, quantized, dropped_term_ids);
            topk_queue topk_1(10);
            TestType op_q(topk_1);
            topk_queue topk_2(10);
            ranked_and_query and_q(topk_2);

            auto scorer = scorer::from_params(ScorerParams(s_name), data->wdata);

            for (auto const& q: data->queries) {
                and_q(make_scored_cursors(data->index, *scorer, q), data->index.num_docs());
                op_q(
                    make_block_max_scored_cursors(data->index, data->wdata, *scorer, q),
                    data->index.num_docs()
                );
                topk_1.finalize();
                topk_2.finalize();
                REQUIRE(topk_1.topk().size() == topk_2.topk().size());
                for (size_t i = 0; i < and_q.topk().size(); ++i) {
                    REQUIRE(
                        topk_1.topk()[i].first == Approx(topk_2.topk()[i].first).epsilon(0.1)
                    );  // tolerance is %
                        // relative
                }
                topk_1.clear();
                topk_2.clear();
            }
        }
    }
}

TEST_CASE("Top k") {
    for (auto&& s_name: {"bm25", "qld"}) {
        std::unordered_set<size_t> dropped_term_ids;
        auto data = IndexData<single_index>::get(s_name, false, dropped_term_ids);
        topk_queue topk_1(10);
        ranked_or_query or_10(topk_1);
        topk_queue topk_2(1);
        ranked_or_query or_1(topk_2);

        auto scorer = scorer::from_params(ScorerParams(s_name), data->wdata);

        for (auto const& q: data->queries) {
            or_10(make_scored_cursors(data->index, *scorer, q), data->index.num_docs());
            or_1(make_scored_cursors(data->index, *scorer, q), data->index.num_docs());
            topk_1.finalize();
            topk_2.finalize();
            if (not or_10.topk().empty()) {
                REQUIRE(not or_1.topk().empty());
                REQUIRE(or_1.topk().front().first == Approx(or_10.topk().front().first).epsilon(0.1));
            }
            topk_1.clear();
            topk_2.clear();
        }
    }
}
