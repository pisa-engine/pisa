#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "test_common.hpp"

#include "pisa_config.hpp"
#include "index_types.hpp"
#include "query/queries.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/block_max_scored_cursor.hpp"

namespace pisa {
namespace test {

struct index_initialization {

    typedef opt_index                                                             index_type;
    typedef wand_data<bm25, wand_data_compressed<bm25, uniform_score_compressor>> WandTypeUniform;
    typedef wand_data<bm25, wand_data_raw<bm25>>                                  WandTypePlain;

    index_initialization()
        : collection(PISA_SOURCE_DIR "/test/test_data/test_collection"),
          document_sizes(PISA_SOURCE_DIR "/test/test_data/test_collection.sizes"),
          wdata(document_sizes.begin()->begin(),
                collection.num_docs(),
                collection,
                partition_type::variable_blocks),
          wdata_fixed(document_sizes.begin()->begin(),
                      collection.num_docs(),
                      collection,
                      partition_type::fixed_blocks),
          wdata_uniform(document_sizes.begin()->begin(),
                        collection.num_docs(),
                        collection,
                        partition_type::variable_blocks) {
        index_type::builder builder(collection.num_docs(), params);
        for (auto const &plist : collection) {
            uint64_t freqs_sum =
                std::accumulate(plist.freqs.begin(), plist.freqs.end(), uint64_t(0));
            builder.add_posting_list(
                plist.docs.size(), plist.docs.begin(), plist.freqs.begin(), freqs_sum);
        }
        builder.build(index);

        term_id_vec   q;
        std::ifstream qfile(PISA_SOURCE_DIR "/test/test_data/queries");
        while (read_query(q, qfile))
            queries.push_back(q);
    }

    global_parameters        params;
    binary_freq_collection   collection;
    binary_collection        document_sizes;
    index_type               index;
    std::vector<term_id_vec> queries;
    WandTypePlain            wdata;
    WandTypePlain            wdata_fixed;
    WandTypeUniform          wdata_uniform;

    template <typename QueryFun>
    void test_against_wand(QueryFun &query_fun) const {
        wand_query or_q(10, index.num_docs());

        for (auto const &q : queries) {
            or_q(make_max_scored_cursors(index, wdata, q));
            auto op_q = query_fun(q);
            REQUIRE(or_q.topk().size() == op_q.topk().size());

            for (size_t i = 0; i < or_q.topk().size(); ++i) {
                REQUIRE(or_q.topk()[i].first == Approx(op_q.topk()[i].first).epsilon(0.01)); // tolerance is % relative
            }
            op_q.clear_topk();
        }
    }
};

} // namespace test
} // namespace pisa

TEST_CASE_METHOD(pisa::test::index_initialization, "block_max_wand_plain") {

    auto query_fun = [&](pisa::term_id_vec terms){
        pisa::block_max_wand_query   block_max_wand_q(10, index.num_docs());
        block_max_wand_q(make_block_max_scored_cursors(index, wdata, terms));
        return block_max_wand_q;
    };
    test_against_wand(query_fun);
}

TEST_CASE_METHOD(pisa::test::index_initialization, "block_max_wand_uniform") {

    auto query_fun = [&](pisa::term_id_vec terms){
        pisa::block_max_wand_query block_max_wand_q(10, index.num_docs());
        block_max_wand_q(make_block_max_scored_cursors(index, wdata_uniform, terms));
        return block_max_wand_q;
    };
    test_against_wand(query_fun);
}

TEST_CASE_METHOD(pisa::test::index_initialization, "block_max_wand_fixed") {

    auto query_fun = [&](pisa::term_id_vec terms){
        pisa::block_max_wand_query   block_max_wand_q(10, index.num_docs());
        block_max_wand_q(make_block_max_scored_cursors(index, wdata_fixed, terms));
        return block_max_wand_q;
    };
    test_against_wand(query_fun);
}
