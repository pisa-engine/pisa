#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "test_common.hpp"

#include "ds2i_config.hpp"
#include "index_types.hpp"
#include "query/queries.hpp"
#include "accumulator/lazy_accumulator.hpp"
#include "cursor/scored_cursor.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/block_max_scored_cursor.hpp"

namespace pisa { namespace test {

    struct index_initialization {

        typedef single_index IndexType;
        typedef wand_data<bm25, wand_data_raw<bm25>> WandType;
        index_initialization()
            : collection(DS2I_SOURCE_DIR "/test/test_data/test_collection")
            , document_sizes(DS2I_SOURCE_DIR "/test/test_data/test_collection.sizes")
            , wdata(document_sizes.begin()->begin(), collection.num_docs(), collection)
        {
            IndexType::builder builder(collection.num_docs(), params);
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

            std::string t;
            std::ifstream tin(DS2I_SOURCE_DIR "/test/test_data/top5_thresholds");
            while (std::getline(tin, t)) {
                thresholds.push_back(std::stof(t));
            }
        }

        global_parameters params;
        binary_freq_collection collection;
        binary_collection document_sizes;
        IndexType index;
        std::vector<term_id_vec> queries;
        std::vector<float> thresholds;
        WandType wdata;

        template <typename QueryFun>
        void test_against_or(QueryFun &query_fun) const
        {
            ranked_or_query<IndexType, WandType> or_q(index, wdata, 10, index.num_docs());

            for (auto const& q: queries) {
                or_q(make_scored_cursors(index, wdata, q));
                auto op_q = query_fun(q);
                REQUIRE(or_q.topk().size() == op_q.topk().size());
                for (size_t i = 0; i < or_q.topk().size(); ++i) {
                    REQUIRE(or_q.topk()[i].first ==
                            Approx(op_q.topk()[i].first).epsilon(0.1)); // tolerance is % relative
                }
            }
        }

        void test_k_size() const
        {
            ranked_or_query<IndexType, WandType> or_10(index, wdata, 10, index.num_docs());
            ranked_or_query<IndexType, WandType> or_1(index, wdata, 1, index.num_docs());

            for (auto const &q : queries) {
                or_10(make_scored_cursors(index, wdata, q));
                or_1(make_scored_cursors(index, wdata, q));
                if (not or_10.topk().empty()) {
                    REQUIRE(not or_1.topk().empty());
                    REQUIRE(or_1.topk().front().first ==
                            Approx(or_10.topk().front().first).epsilon(0.1));
                }
            }
        }
    };

}}

TEST_CASE_METHOD(pisa::test::index_initialization, "wand")
{
    auto query_fun = [&](pisa::term_id_vec terms){
        pisa::wand_query<IndexType, WandType> wand_q(index, wdata, 10, index.num_docs());
        wand_q(make_max_scored_cursors(index, wdata, terms));
        return wand_q;
    };
    test_against_or(query_fun);
}

TEST_CASE_METHOD(pisa::test::index_initialization, "maxscore")
{
    auto query_fun = [&](pisa::term_id_vec terms){
        pisa::maxscore_query<IndexType, WandType> maxscore_q(index, wdata, 10, index.num_docs());
        maxscore_q(make_max_scored_cursors(index, wdata, terms));
        return maxscore_q;
    };
    test_against_or(query_fun);
}

TEST_CASE_METHOD(pisa::test::index_initialization, "block_max_maxscore")
{
    auto query_fun = [&](pisa::term_id_vec terms){
        pisa::block_max_maxscore_query<IndexType, WandType> block_max_maxscore_q(index, wdata, 10, index.num_docs());
        block_max_maxscore_q(make_block_max_scored_cursors(index, wdata, terms));
        return block_max_maxscore_q;
    };
    test_against_or(query_fun);
}

TEST_CASE_METHOD(pisa::test::index_initialization, "ranked_or_taat")
{

    auto query_fun = [&](pisa::term_id_vec terms){
        pisa::ranked_or_taat_query<IndexType, WandType, pisa::Simple_Accumulator> ranked_or_taat_q(index, wdata, 10, index.num_docs());
        ranked_or_taat_q(make_scored_cursors(index, wdata, terms));
        return ranked_or_taat_q;
    };
    test_against_or(query_fun);
}

TEST_CASE_METHOD(pisa::test::index_initialization, "ranked_or_taat_lazy")
{
    auto query_fun = [&](pisa::term_id_vec terms){
        pisa::ranked_or_taat_query<IndexType, WandType, pisa::Lazy_Accumulator<4>> ranked_or_taat_q(index, wdata, 10, index.num_docs());
        ranked_or_taat_q(make_scored_cursors(index, wdata, terms));
        return ranked_or_taat_q;
    };
    test_against_or(query_fun);
}

TEST_CASE_METHOD(pisa::test::index_initialization, "topk_size_ranked_or")
{
    test_k_size();
}
