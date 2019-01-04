#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "test_common.hpp"

#include "ds2i_config.hpp"
#include "index_types.hpp"
#include "query/queries.hpp"

namespace ds2i { namespace test {

    struct index_initialization {

        typedef single_index index_type;
        typedef wand_data<bm25, wand_data_raw<bm25>> WandType;
        index_initialization()
            : collection(DS2I_SOURCE_DIR "/test/test_data/test_collection")
            , document_sizes(DS2I_SOURCE_DIR "/test/test_data/test_collection.sizes")
            , wdata(document_sizes.begin()->begin(), collection.num_docs(), collection)
        {
            index_type::builder builder(collection.num_docs(), params);
            for (auto const& plist: collection) {
                uint64_t freqs_sum = std::accumulate(plist.freqs.begin(),
                                                     plist.freqs.end(), uint64_t(0));
                builder.add_posting_list(plist.docs.size(), plist.docs.begin(),
                                         plist.freqs.begin(), freqs_sum);
            }
            builder.build(index);

            term_id_vec q;
            std::ifstream qfile(DS2I_SOURCE_DIR "/test/test_data/queries");
            while (read_query(q, qfile)) queries.push_back(q);
        }

        global_parameters params;
        binary_freq_collection collection;
        binary_collection document_sizes;
        index_type index;
        std::vector<term_id_vec> queries;
        WandType wdata;

        template <typename QueryOp>
        void test_against_or(QueryOp& op_q) const
        {
            ranked_or_query<WandType> or_q(wdata, 10);

            for (auto const& q: queries) {
                or_q(index, q);
                op_q(index, q);
                REQUIRE(or_q.topk().size() == op_q.topk().size());
                for (size_t i = 0; i < or_q.topk().size(); ++i) {
                    REQUIRE(or_q.topk()[i].first == Approx(op_q.topk()[i].first).epsilon(0.1)); // tolerance is % relative
                }
            }
        }


    };

}}


TEST_CASE_METHOD(ds2i::test::index_initialization, "wand")
{
    ds2i::wand_query<WandType> wand_q(wdata, 10);
    test_against_or(wand_q);
}

TEST_CASE_METHOD(ds2i::test::index_initialization, "maxscore")
{
    ds2i::maxscore_query<WandType> maxscore_q(wdata, 10);
    test_against_or(maxscore_q);
}

TEST_CASE_METHOD(ds2i::test::index_initialization, "block_max_maxscore")
{
    ds2i::block_max_maxscore_query<WandType> bmm_q(wdata, 10);
    test_against_or(bmm_q);
}