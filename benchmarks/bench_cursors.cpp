#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>

#include "accumulator/lazy_accumulator.hpp"
#include "cursor/block_max_scored_cursor.hpp"
#include "cursor/scored_cursor.hpp"
#include "index_types.hpp"
#include "io.hpp"
#include "pisa_config.hpp"
#include "query/algorithm.hpp"

using pisa::binary_collection;
using pisa::binary_freq_collection;
using pisa::block_max_maxscore_query;
using pisa::block_max_scored_cursor;
using pisa::block_max_wand_query;
using pisa::BlockSize;
using pisa::FixedBlock;
using pisa::global_parameters;
using pisa::Lazy_Accumulator;
using pisa::make_block_max_scored_cursors;
using pisa::maxscore_query;
using pisa::parse_query_ids;
using pisa::Query;
using pisa::ranked_or_query;
using pisa::ranked_or_taat_query;
using pisa::Simple_Accumulator;
using pisa::single_index;
using pisa::term_id_type;
using pisa::term_id_vec;
using pisa::topk_queue;
using pisa::wand_data;
using pisa::wand_data_raw;
using pisa::wand_query;
using pisa::io::for_each_line;

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
        typename Index::builder builder(collection.num_docs(), params);
        for (auto const& plist: collection) {
            uint64_t freqs_sum = std::accumulate(plist.freqs.begin(), plist.freqs.end(), uint64_t(0));
            builder.add_posting_list(
                plist.docs.size(), plist.docs.begin(), plist.freqs.begin(), freqs_sum);
        }
        builder.build(index);

        term_id_vec q;
        std::ifstream qfile(PISA_SOURCE_DIR "/test/test_data/queries");
        auto push_query = [&](std::string const& query_line) {
            queries.push_back(parse_query_ids(query_line));
        };
        for_each_line(qfile, push_query);

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

// NOLINTNEXTLINE(hicpp-explicit-conversions)
TEMPLATE_TEST_CASE(
    "Ranked query test",
    "[query][ranked][integration]",
    ranked_or_taat_query_acc<Simple_Accumulator>,
    ranked_or_taat_query_acc<Lazy_Accumulator<4>>,
    wand_query,
    maxscore_query,
    block_max_wand_query,
    block_max_maxscore_query)
{
    auto data = IndexData<single_index>::get("bm25", false, {});
    topk_queue topk(10);
    TestType query_algorithm(topk);
    auto scorer = pisa::scorer::from_params(ScorerParams("bm25"), data->wdata);

    auto run = [&](auto&& query) {
        query_algorithm(
            make_block_max_scored_cursors(data->index, data->wdata, *scorer, query),
            data->index.num_docs());
        topk.finalize();
        return topk;
    };

    BENCHMARK("One-term query")
    {
        return run(Query{
            .id = std::nullopt, .terms = std::vector<term_id_type>{33726}, .term_weights = {1.0}});
    };

    BENCHMARK("Two-term query")
    {
        return run(Query{.id = std::nullopt,
                         .terms = std::vector<term_id_type>{40429, 86328},
                         .term_weights = {1.0, 1.0}});
    };

    BENCHMARK("Three-term query")
    {
        return run(Query{.id = std::nullopt,
                         .terms = std::vector<term_id_type>{106967, 552, 59184},
                         .term_weights = {1.0, 1.0, 1.0}});
    };

    BENCHMARK("Eight-term query")
    {
        return run(Query{.id = std::nullopt,
                         .terms =
                             std::vector<term_id_type>{
                                 110717, 76695, 110770, 74156, 102912, 54599, 42353, 111450},
                         .term_weights = {1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0}});
    };
}
