#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "test_common.hpp"

#include "ds2i_config.hpp"
#include "index_types.hpp"
#include "query/queries.hpp"

namespace pisa { namespace test {

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

            std::string t;
            std::ifstream tin(DS2I_SOURCE_DIR "/test/test_data/top5_thresholds");
            while (std::getline(tin, t)) {
                thresholds.push_back(std::stof(t));
            }
        }

        global_parameters params;
        binary_freq_collection collection;
        binary_collection document_sizes;
        index_type index;
        std::vector<term_id_vec> queries;
        std::vector<float> thresholds;
        WandType wdata;

        template <typename QueryOp>
        void test_against_or(QueryOp &op_q) const
        {
            ranked_or_query<WandType> or_q(wdata, 10);

            for (auto const &q : queries) {
                or_q(index, q);
                op_q(index, q);
                REQUIRE(or_q.topk().size() == op_q.topk().size());
                for (size_t i = 0; i < or_q.topk().size(); ++i) {
                    REQUIRE(or_q.topk()[i].first ==
                            Approx(op_q.topk()[i].first).epsilon(0.1)); // tolerance is % relative
                }
            }
        }

        void test_k_size() const
        {
            ranked_or_query<WandType> or_10(wdata, 10);
            ranked_or_query<WandType> or_1(wdata, 1);

            for (auto const &q : queries) {
                or_10(index, q);
                or_1(index, q);
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
    pisa::wand_query<WandType> wand_q(wdata, 10);
    test_against_or(wand_q);
}

TEST_CASE_METHOD(pisa::test::index_initialization, "maxscore")
{
    pisa::maxscore_query<WandType> maxscore_q(wdata, 10);
    test_against_or(maxscore_q);
}

TEST_CASE_METHOD(pisa::test::index_initialization, "block_max_maxscore")
{
    pisa::block_max_maxscore_query<WandType> bmm_q(wdata, 10);
    test_against_or(bmm_q);
}

/// Issue #26 https://github.com/pisa-engine/pisa/issues/26
TEST_CASE_METHOD(pisa::test::index_initialization, "topk_size_ranked_or")
{
    test_k_size();
}
