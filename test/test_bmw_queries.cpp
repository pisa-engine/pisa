#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "test_common.hpp"

#include "ds2i_config.hpp"
#include "index_types.hpp"
#include "query/queries.hpp"
#include "query/scored_range.hpp"

namespace pisa {
namespace test {

template<typename Index = opt_index>
struct index_initialization {

    using index_type = Index;
    typedef wand_data<bm25, wand_data_compressed<bm25, uniform_score_compressor>> WandTypeUniform;
    typedef wand_data<bm25, wand_data_raw<bm25>>                                  WandTypePlain;

    index_initialization()
        : collection(DS2I_SOURCE_DIR "/test/test_data/test_collection"),
          document_sizes(DS2I_SOURCE_DIR "/test/test_data/test_collection.sizes"),
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
        typename index_type::builder builder(collection.num_docs(), params);
        for (auto const &plist : collection) {
            uint64_t freqs_sum =
                std::accumulate(plist.freqs.begin(), plist.freqs.end(), uint64_t(0));
            builder.add_posting_list(
                plist.docs.size(), plist.docs.begin(), plist.freqs.begin(), freqs_sum);
        }
        builder.build(index);

        term_id_vec   q;
        std::ifstream qfile(DS2I_SOURCE_DIR "/test/test_data/queries");
        while (read_query(q, qfile)) {
            queries.push_back(q);
        }
    }

    global_parameters        params;
    binary_freq_collection   collection;
    binary_collection        document_sizes;
    index_type               index;
    std::vector<term_id_vec> queries;
    WandTypePlain            wdata;
    WandTypePlain            wdata_fixed;
    WandTypeUniform          wdata_uniform;

    template <typename QueryOp>
    void test_against_wand(QueryOp &op_q) const {
        wand_query<index_type, WandTypePlain> or_q(index, wdata, 10);

        for (auto const &q : queries) {
            or_q(q);
            op_q(q);
            REQUIRE(or_q.topk().size() == op_q.topk().size());

            for (size_t i = 0; i < or_q.topk().size(); ++i) {
                REQUIRE(or_q.topk()[i].first == Approx(op_q.topk()[i].first).epsilon(0.01)); // tolerance is % relative
            }
            op_q.clear_topk();
        }
    }

    template <typename QueryOp>
    void test_with_ranges(QueryOp &op_q) const
    {
        wand_query<index_type, WandTypePlain> or_q(index, wdata, 10);

        for (auto const &q : queries) {
            or_q(q);
            op_q(gsl::make_span(block_max_scored_ranges(index, wdata, q)));
            REQUIRE(or_q.topk().size() == op_q.topk().size());
            for (size_t i = 0; i < or_q.topk().size(); ++i) {
                REQUIRE(or_q.topk()[i].first ==
                        Approx(op_q.topk()[i].first).epsilon(0.1)); // tolerance is % relative
            }
            op_q.clear_topk();
        }
    }
};

} // namespace test
} // namespace pisa

TEST_CASE_METHOD(pisa::test::index_initialization<pisa::block_simdbp_index>, "block_max_wand")
{
    pisa::block_max_wand_query<index_type, WandTypePlain>   block_max_wand_q(index, wdata, 10);
    pisa::block_max_wand_query<index_type, WandTypeUniform> block_max_wand_uniform_q(index, wdata_uniform, 10);
    pisa::block_max_wand_query<index_type, WandTypePlain>   block_max_wand_fixed_q(index, wdata_fixed, 10);
    test_against_wand(block_max_wand_uniform_q);
    test_against_wand(block_max_wand_q);
    test_against_wand(block_max_wand_fixed_q);
}

TEST_CASE_METHOD(pisa::test::index_initialization<pisa::block_simdbp_index>,
                 "block_max_wand: ranges")
{
    pisa::block_max_wand_query<index_type, WandTypePlain>   block_max_wand_q(index, wdata, 10);
    pisa::block_max_wand_query<index_type, WandTypeUniform> block_max_wand_uniform_q(index, wdata_uniform, 10);
    pisa::block_max_wand_query<index_type, WandTypePlain>   block_max_wand_fixed_q(index, wdata_fixed, 10);
    test_with_ranges(block_max_wand_q);
    test_with_ranges(block_max_wand_uniform_q);
    test_with_ranges(block_max_wand_fixed_q);
}
