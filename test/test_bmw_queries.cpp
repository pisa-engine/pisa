#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "test_common.hpp"

#include "pisa_config.hpp"
#include "index_types.hpp"
#include "query/queries.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/block_max_scored_cursor.hpp"

using namespace pisa;

using WandTypeUniform = wand_data<bm25, wand_data_compressed<bm25, uniform_score_compressor>>;
using WandTypePlain = wand_data<bm25, wand_data_raw<bm25>>;
using WandTypeUniformLs = wand_data<bm25, wand_data_compressed_ls<bm25, uniform_score_compressor_ls>>;
using WandTypePlainLsr = wand_data<bm25, wand_data_raw<bm25, true>>;
using WandTypeUniformLsr = wand_data<bm25, wand_data_compressed<bm25, uniform_score_compressor, true>>;

template <typename Index>
struct IndexData {

    static std::unique_ptr<IndexData> data;

    IndexData()
        : collection(PISA_SOURCE_DIR "/test/test_data/test_collection"),
          document_sizes(PISA_SOURCE_DIR "/test/test_data/test_collection.sizes"),
          wdata(document_sizes.begin()->begin(),
                collection.num_docs(),
                collection,
                partition_type::variable_blocks)
    {
        typename Index::builder builder(collection.num_docs(), params);
        for (auto const &plist : collection) {
            uint64_t freqs_sum =
                std::accumulate(plist.freqs.begin(), plist.freqs.end(), uint64_t(0));
            builder.add_posting_list(
                plist.docs.size(), plist.docs.begin(), plist.freqs.begin(), freqs_sum);
        }
        builder.build(index);

        term_id_vec q;
        std::ifstream qfile(PISA_SOURCE_DIR "/test/test_data/queries");
        while (read_query(q, qfile)) {
            queries.push_back(q);
        }
    }

    global_parameters params;
    binary_freq_collection collection;
    binary_collection document_sizes;
    Index index;
    std::vector<term_id_vec> queries;
    WandTypePlain wdata;

    [[nodiscard]] static auto get()
    {
        if (IndexData::data == nullptr) {
            IndexData::data = std::make_unique<IndexData<Index>>();
        }
        return IndexData::data.get();
    }
};

template <typename Index>
std::unique_ptr<IndexData<Index>> IndexData<Index>::data = nullptr;

template <typename Wand>
auto test(Wand &wdata)
{
    auto data = IndexData<single_index>::get();
    block_max_wand_query op_q(10);
    wand_query wand_q(10);

    for (auto const &q : data->queries) {
        wand_q(make_max_scored_cursors(data->index, data->wdata, q), data->index.num_docs());
        op_q(make_block_max_scored_cursors(data->index, wdata, q), data->index.num_docs());
        REQUIRE(wand_q.topk().size() == op_q.topk().size());

        for (size_t i = 0; i < wand_q.topk().size(); ++i) {
            REQUIRE(wand_q.topk()[i].first ==
                    Approx(op_q.topk()[i].first).epsilon(0.01)); // tolerance is % relative
        }
        op_q.clear_topk();
    }
}

TEST_CASE("block_max_wand", "[bmw][query][ranked][integration]", )
{
    auto data = IndexData<single_index>::get();

    SECTION("Regular") { test(data->wdata); }
    SECTION("Fixed")
    {
        WandTypePlain wdata_fixed(data->document_sizes.begin()->begin(),
                                  data->collection.num_docs(),
                                  data->collection,
                                  partition_type::fixed_blocks);
        test(wdata_fixed);
    }
    SECTION("Uniform")
    {
        WandTypeUniform wdata_uniform(data->document_sizes.begin()->begin(),
                                      data->collection.num_docs(),
                                      data->collection,
                                      partition_type::variable_blocks);
        test(wdata_uniform);
    }

    SECTION("compressed longer-skipping")
    {
        WandTypeUniformLs wdata_uniform_ls(data->document_sizes.begin()->begin(),
                                      data->collection.num_docs(),
                                      data->collection,
                                      partition_type::variable_blocks);
        test(wdata_uniform_ls);
    }

    SECTION("Plain longer-skipping runtime")
    {
        WandTypePlainLsr wdata_plain_ls_runtime(data->document_sizes.begin()->begin(),
                                      data->collection.num_docs(),
                                      data->collection,
                                      partition_type::variable_blocks);
        test(wdata_plain_ls_runtime);
    }

    SECTION("compressed longer-skipping runtime")
    {
        WandTypeUniformLsr wdata_uniform_ls_runtime(data->document_sizes.begin()->begin(),
                                      data->collection.num_docs(),
                                      data->collection,
                                      partition_type::variable_blocks);
        test(wdata_uniform_ls_runtime);
    }
}
