#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <unordered_map>

#include "test_common.hpp"

#include "cursor/block_max_scored_cursor.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/scored_cursor.hpp"
#include "index_types.hpp"
#include "pisa_config.hpp"
#include "query/algorithm.hpp"
#include "wand_data.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

using namespace pisa;

using WandTypeUniform = wand_data<wand_data_compressed<>>;
using WandTypePlain = wand_data<wand_data_raw>;

template <typename Index>
struct IndexData {
    static std::unordered_map<std::string, std::unique_ptr<IndexData>> data;

    IndexData(std::string const& scorer_name, std::unordered_set<size_t> const& dropped_term_ids)
        : collection(PISA_SOURCE_DIR "/test/test_data/test_collection"),
          document_sizes(PISA_SOURCE_DIR "/test/test_data/test_collection.sizes"),
          wdata(
              document_sizes.begin()->begin(),
              collection.num_docs(),
              collection,
              ScorerParams(scorer_name),
              BlockSize(VariableBlock(12.0)),
              false,
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
        io::for_each_line(qfile, push_query);
    }

    global_parameters params;
    binary_freq_collection collection;
    binary_collection document_sizes;
    Index index;
    std::vector<Query> queries;
    WandTypePlain wdata;

    [[nodiscard]] static auto
    get(std::string const& s_name, std::unordered_set<size_t> const& dropped_term_ids)
    {
        if (IndexData::data.find(s_name) == IndexData::data.end()) {
            IndexData::data[s_name] = std::make_unique<IndexData<Index>>(s_name, dropped_term_ids);
        }
        return IndexData::data[s_name].get();
    }
};

template <typename Index>
std::unordered_map<std::string, unique_ptr<IndexData<Index>>> IndexData<Index>::data = {};

template <typename Wand>
auto test(Wand& wdata, std::string const& s_name)
{
    std::unordered_set<size_t> dropped_term_ids;
    auto data = IndexData<single_index>::get(s_name, dropped_term_ids);
    topk_queue topk_1(10);
    block_max_wand_query op_q(topk_1);
    topk_queue topk_2(10);
    wand_query wand_q(topk_2);
    auto scorer = scorer::from_params(ScorerParams(s_name), data->wdata);

    for (auto const& q: data->queries) {
        wand_q(make_max_scored_cursors(data->index, data->wdata, *scorer, q), data->index.num_docs());
        op_q(make_block_max_scored_cursors(data->index, wdata, *scorer, q), data->index.num_docs());
        topk_1.finalize();
        topk_2.finalize();
        REQUIRE(topk_2.topk().size() == topk_1.topk().size());

        for (size_t i = 0; i < wand_q.topk().size(); ++i) {
            REQUIRE(
                topk_2.topk()[i].first == Approx(topk_1.topk()[i].first).epsilon(0.01));  // tolerance
                                                                                          // is
                                                                                          // %
                                                                                          // relative
        }
        topk_1.clear();
        topk_2.clear();
    }
}

TEST_CASE("block_max_wand", "[bmw][query][ranked][integration]", )
{
    for (auto&& s_name: {"bm25", "qld"}) {
        std::unordered_set<size_t> dropped_term_ids;
        auto data = IndexData<single_index>::get(s_name, dropped_term_ids);

        SECTION("Regular") { test(data->wdata, s_name); }
        SECTION("Fixed")
        {
            std::unordered_set<size_t> dropped_term_ids;
            WandTypePlain wdata_fixed(
                data->document_sizes.begin()->begin(),
                data->collection.num_docs(),
                data->collection,
                ScorerParams(s_name),
                BlockSize(FixedBlock(5)),
                false,
                dropped_term_ids);
            test(wdata_fixed, s_name);
        }
        SECTION("Uniform")
        {
            std::unordered_set<size_t> dropped_term_ids;
            WandTypeUniform wdata_uniform(
                data->document_sizes.begin()->begin(),
                data->collection.num_docs(),
                data->collection,
                ScorerParams(s_name),
                BlockSize(VariableBlock(12.0)),
                false,
                dropped_term_ids);
            test(wdata_uniform, s_name);
        }
    }
}
