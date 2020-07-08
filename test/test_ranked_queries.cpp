#define CATCH_CONFIG_MAIN

#include <catch2/catch.hpp>
#include <functional>

#include "accumulator/lazy_accumulator.hpp"
#include "configuration.hpp"
#include "cursor/block_max_scored_cursor.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/range_block_max_scored_cursor.hpp"
#include "cursor/scored_cursor.hpp"
#include "index_types.hpp"
#include "linear_quantizer.hpp"
#include "pisa_config.hpp"
#include "query/algorithm.hpp"
#include "query/live_block_computation.hpp"
#include "test_common.hpp"
#include "wand_data_range.hpp"

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
              quantized,
              dropped_term_ids)

    {
        std::unique_ptr<index_scorer<wand_data<wand_data_raw>>> scorer;
        if (quantized) {
            scorer = scorer::from_params(ScorerParams(scorer_name), wdata);
        }
        typename Index::builder builder(collection.num_docs(), params);
        size_t term_id = 0;

        for (auto const& plist: collection) {
            size_t size = plist.docs.size();
            if (quantized) {
                LinearQuantizer quantizer(
                    wdata.index_max_term_weight(), configuration::get().quantization_bits);
                auto term_scorer = scorer->term_scorer(term_id);
                std::vector<uint64_t> quants;
                for (size_t pos = 0; pos < size; ++pos) {
                    uint64_t doc = *(plist.docs.begin() + pos);
                    uint64_t freq = *(plist.freqs.begin() + pos);
                    float score = term_scorer(doc, freq);
                    uint64_t quant_score = quantizer(score);
                    quants.push_back(quant_score);
                }
                assert(quants.size() == size);
                uint64_t quants_sum =
                    std::accumulate(quants.begin(), quants.begin() + quants.size(), uint64_t(0));
                builder.add_posting_list(size, plist.docs.begin(), quants.begin(), quants_sum);
            } else {
                uint64_t freqs_sum =
                    std::accumulate(plist.freqs.begin(), plist.freqs.end(), uint64_t(0));
                builder.add_posting_list(
                    plist.docs.size(), plist.docs.begin(), plist.freqs.begin(), freqs_sum);
            }
            term_id += 1;
        }
        builder.build(index);

        term_id_vec q;
        std::ifstream qfile(PISA_SOURCE_DIR "/test/test_data/queries");
        auto push_query = [&](std::string const& query_line) {
            queries.push_back(parse_query_ids(query_line));
        };
        io::for_each_line(qfile, push_query);

        std::string t;
    }

    [[nodiscard]] static auto
    get(std::string const& s_name, bool quantized, std::unordered_set<size_t> const& dropped_term_ids)
    {
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
std::unordered_map<std::string, unique_ptr<IndexData<Index>>> IndexData<Index>::data = {};

template <typename Acc>
class ranked_or_taat_query_acc: public ranked_or_taat_query {
  public:
    using ranked_or_taat_query::ranked_or_taat_query;

    template <typename CursorRange>
    void operator()(CursorRange&& cursors, uint64_t max_docid)
    {
        Acc accumulator(max_docid);
        ranked_or_taat_query::operator()(cursors, max_docid, accumulator);
    }
};

// template <typename T>
// class range_query_128: public range_query<T> {
//   public:
//     using range_query<T>::range_query;

//     template <typename CursorRange>
//     void operator()(CursorRange&& cursors, uint64_t max_docid)
//     {
//         range_query<T>::operator()(cursors, max_docid, 128);
//     }
// };

// NOLINTNEXTLINE(hicpp-explicit-conversions)
TEMPLATE_TEST_CASE(
    "Ranked query test",
    "[query][ranked][integration]",
    ranked_or_taat_query_acc<Simple_Accumulator>,
    ranked_or_taat_query_acc<Lazy_Accumulator<4>>,
    wand_query,
    maxscore_query,
    block_max_wand_query,
    block_max_maxscore_query
    // range_query_128<ranked_or_taat_query_acc<Simple_Accumulator>>,
    // range_query_128<ranked_or_taat_query_acc<Lazy_Accumulator<4>>>,
    // range_query_128<wand_query>,
    // range_query_128<maxscore_query>,
    // range_query_128<block_max_wand_query>,
    // range_query_128<block_max_maxscore_query>
)
{
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
                    data->index.num_docs());
                topk_1.finalize();
                topk_2.finalize();
                REQUIRE(topk_2.topk().size() == topk_1.topk().size());
                for (size_t i = 0; i < topk_2.topk().size(); ++i) {
                    REQUIRE(
                        topk_2.topk()[i].first
                        == Approx(topk_1.topk()[i].first).epsilon(0.1));  // tolerance is %
                                                                          // relative
                }
                topk_1.clear();
                topk_2.clear();
            }
        }
    }
}

// NOLINTNEXTLINE(hicpp-explicit-conversions)
TEMPLATE_TEST_CASE("BMW-LB test", "[query][ranked][range][integration]", block_max_wand_lb_query)
{
    auto quantized = true;
    for (auto&& s_name: {"bm25"}) {
        std::unordered_set<size_t> dropped_term_ids;
        auto data = IndexData<single_index>::get(s_name, quantized, dropped_term_ids);
        topk_queue topk_1(10);
        TestType op_q(topk_1);
        topk_queue topk_2(10);
        ranked_or_query or_q(topk_2);
        auto scorer_name = quantized ? "quantized" : s_name;
        auto scorer = scorer::from_params(ScorerParams(scorer_name), data->wdata);

        constexpr size_t range_size = 128;
        std::map<uint32_t, std::vector<uint8_t>> term_enum;
        size_t blocks_num = ceil_div(data->index.num_docs(), range_size);
        for (auto const& q: data->queries) {
            for (auto t: q.terms) {
                auto docs_enum = data->index[t];
                auto s = scorer->term_scorer(t);
                auto tmp = wand_data_range<range_size, 0>::compute_block_max_scores(
                    docs_enum, s, blocks_num);
                term_enum[t] = std::vector<uint8_t>(tmp.begin(), tmp.end());
            }
        }

        for (auto const& q: data->queries) {
            or_q(make_scored_cursors(data->index, *scorer, q), data->index.num_docs());
            topk_2.finalize();

            std::vector<std::vector<uint8_t>> scores;
            for (auto&& t: q.terms) {
                scores.emplace_back(term_enum[t].begin(), term_enum[t].end());
            }
            auto live_blocks_bv = compute_live_quant16(scores, topk_2.threshold());

            op_q(
                make_range_block_max_scored_cursors(data->index, data->wdata, *scorer, q, term_enum),
                data->index.num_docs(),
                live_blocks_bv,
                range_size);
            topk_1.finalize();

            CHECK(topk_2.topk().size() == topk_1.topk().size());
            for (size_t i = 0; i < topk_2.topk().size(); ++i) {
                CHECK(topk_2.topk()[i].first == Approx(topk_1.topk()[i].first).epsilon(0.1));  // tolerance
                                                                                               // is
                                                                                               // %
                                                                                               // relative
            }
            topk_1.clear();
            topk_2.clear();
        }
    }
}

// NOLINTNEXTLINE(hicpp-explicit-conversions)
TEST_CASE("Ranked range-maxscore test", "[query][ranked][range][integration]")
{
    auto quantized = true;
    for (auto&& s_name: {"bm25"}) {
        std::unordered_set<size_t> dropped_term_ids;
        auto data = IndexData<single_index>::get(s_name, quantized, dropped_term_ids);
        topk_queue topk_1(10);
        range_query<maxscore_query> op_q(topk_1);
        topk_queue topk_2(10);
        ranked_or_query or_q(topk_2);

        auto scorer_name = quantized ? "quantized" : s_name;
        auto scorer = scorer::from_params(ScorerParams(scorer_name), data->wdata);

        constexpr size_t range_size = 128;
        std::map<uint32_t, std::vector<uint8_t>> term_enum;
        size_t blocks_num = ceil_div(data->index.num_docs(), range_size);
        for (auto const& q: data->queries) {
            for (auto t: q.terms) {
                auto docs_enum = data->index[t];
                auto s = scorer->term_scorer(t);
                auto tmp = wand_data_range<range_size, 0>::compute_block_max_scores(
                    docs_enum, s, blocks_num);
                term_enum[t] = std::vector<uint8_t>(tmp.begin(), tmp.end());
            }
        }

        for (auto const& q: data->queries) {
            or_q(make_scored_cursors(data->index, *scorer, q), data->index.num_docs());
            topk_2.finalize();

            std::vector<std::vector<uint8_t>> scores;
            for (auto&& t: q.terms) {
                scores.emplace_back(term_enum[t].begin(), term_enum[t].end());
            }
            auto live_blocks_bv = compute_live_quant16(scores, topk_2.threshold());

            op_q(
                make_range_block_max_scored_cursors(data->index, data->wdata, *scorer, q, term_enum),
                data->index.num_docs(),
                range_size,
                live_blocks_bv);
            topk_1.finalize();

            REQUIRE(topk_2.topk().size() == topk_1.topk().size());
            for (size_t i = 0; i < topk_2.topk().size(); ++i) {
                REQUIRE(topk_2.topk()[i].first == topk_1.topk()[i].first);
            }
            topk_1.clear();
            topk_2.clear();
        }
    }
}

// NOLINTNEXTLINE(hicpp-explicit-conversions)
TEST_CASE("Ranked range or-taat query test", "[query][ranked][range][integration]", )
{
    auto quantized = true;
    for (auto&& s_name: {"bm25"}) {
        constexpr size_t range_size = 128;

        std::unordered_set<size_t> dropped_term_ids;
        auto data = IndexData<single_index>::get(s_name, quantized, dropped_term_ids);
        topk_queue topk_2(10);
        ranked_or_query or_q(topk_2);

        auto scorer_name = quantized ? "quantized" : s_name;
        auto scorer = scorer::from_params(ScorerParams(scorer_name), data->wdata);

        std::map<uint32_t, std::vector<uint8_t>> term_enum;
        size_t blocks_num = ceil_div(data->index.num_docs(), range_size);
        for (auto const& q: data->queries) {
            for (auto t: q.terms) {
                auto docs_enum = data->index[t];
                auto s = scorer->term_scorer(t);
                auto tmp = wand_data_range<range_size, 0>::compute_block_max_scores(
                    docs_enum, s, blocks_num);
                term_enum[t] = std::vector<uint8_t>(tmp.begin(), tmp.end());
            }
        }

        for (auto const& q: data->queries) {
            or_q(make_scored_cursors(data->index, *scorer, q), data->index.num_docs());
            topk_2.finalize();

            std::vector<std::vector<uint8_t>> scores;
            for (auto&& t: q.terms) {
                scores.emplace_back(term_enum[t].begin(), term_enum[t].end());
            }
            auto live_blocks_bv = compute_live_quant16(scores, 0);

            range_or_taat_query<range_size> op_q(10, topk_2.threshold());
            std::vector<uint16_t> topk_vector(10'000);
            std::vector<uint32_t> topdoc_vector(10'000);
            op_q(
                make_range_block_max_scored_cursors(data->index, data->wdata, *scorer, q, term_enum),
                data->index.num_docs(),
                live_blocks_bv,
                topk_vector,
                topdoc_vector);

            REQUIRE(topk_2.topk().size() == topk_vector.size());
            for (size_t i = 0; i < topk_2.topk().size(); ++i) {
                REQUIRE(topk_2.topk()[i].first == topk_vector[i]);
            }
            topk_2.clear();
        }
    }
}

// NOLINTNEXTLINE(hicpp-explicit-conversions)
TEMPLATE_TEST_CASE("Ranked AND query test", "[query][ranked][integration]", block_max_ranked_and_query)
{
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
                    data->index.num_docs());
                topk_1.finalize();
                topk_2.finalize();
                REQUIRE(topk_1.topk().size() == topk_2.topk().size());
                for (size_t i = 0; i < and_q.topk().size(); ++i) {
                    REQUIRE(
                        topk_1.topk()[i].first
                        == Approx(topk_2.topk()[i].first).epsilon(0.1));  // tolerance is %
                                                                          // relative
                }
                topk_1.clear();
                topk_2.clear();
            }
        }
    }
}

TEST_CASE("Top k")
{
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
