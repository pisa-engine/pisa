#define CATCH_CONFIG_MAIN
#include <boost/te.hpp>
#include <catch2/catch.hpp>

#include "test_common.hpp"

#include "pisa_config.hpp"
#include "index_types.hpp"
#include "query/queries.hpp"
#include "accumulator/lazy_accumulator.hpp"
#include "cursor/scored_cursor.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/block_max_scored_cursor.hpp"

namespace pisa { namespace test {

    template <typename QueryFun>
    struct index_initialization {

        using IndexType = single_index;
        using WandType = wand_data<bm25, wand_data_raw<bm25>>;
        index_initialization()
            : collection(PISA_SOURCE_DIR "/test/test_data/test_collection"),
              document_sizes(PISA_SOURCE_DIR "/test/test_data/test_collection.sizes"),
              wdata(document_sizes.begin()->begin(), collection.num_docs(), collection)
        {
            IndexType::builder builder(collection.num_docs(), params);
            for (auto const& plist: collection) {
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

            std::string t;
            std::ifstream tin(PISA_SOURCE_DIR "/test/test_data/top5_thresholds");
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

        void test_against_or(QueryFun &&op_q) const
        {
            ranked_or_query or_q(10);

            for (auto const& q: queries) {
                or_q(make_scored_cursors(index, wdata, q), index.num_docs());
                op_q(make_block_max_scored_cursors(index, wdata, q), index.num_docs());
                REQUIRE(or_q.topk().size() == op_q.topk().size());
                for (size_t i = 0; i < or_q.topk().size(); ++i) {
                    REQUIRE(or_q.topk()[i].first ==
                            Approx(op_q.topk()[i].first).epsilon(0.1)); // tolerance is % relative
                }
            }
        }

        void test_k_size() const
        {
            ranked_or_query or_10(10);
            ranked_or_query or_1(1);

            for (auto const &q : queries) {
                or_10(make_scored_cursors(index, wdata, q), index.num_docs());
                or_1(make_scored_cursors(index, wdata, q), index.num_docs());
                if (not or_10.topk().empty()) {
                    REQUIRE(not or_1.topk().empty());
                    REQUIRE(or_1.topk().front().first ==
                            Approx(or_10.topk().front().first).epsilon(0.1));
                }
            }
        }
    };

}}

using namespace pisa;

TEMPLATE_TEST_CASE_METHOD(test::index_initialization,
                          "Ranked query test",
                          "",
                          ranked_or_taat_query<Simple_Accumulator>,
                          ranked_or_taat_query<Lazy_Accumulator<4>>,
                          wand_query,
                          maxscore_query,
                          block_max_wand_query,
                          block_max_maxscore_query)
{
    using super = test::index_initialization<TestType>;
    super::test_against_or(TestType(10));
}




TEST_CASE_METHOD(test::index_initialization<ranked_or_query>, "Ranked query test")
{
    test_k_size();
}

TEMPLATE_TEST_CASE_METHOD(test::index_initialization,
                          "Ranged ranked query test",
                          "",
                          range_query<ranked_or_taat_query<Simple_Accumulator>>,
                          range_query<ranked_or_taat_query<Lazy_Accumulator<4>>>,
                          range_query<wand_query>,
                          range_query<maxscore_query>,
                          range_query<block_max_wand_query>,
                          range_query<block_max_maxscore_query>)
{
    using super = test::index_initialization<TestType>;
    super::test_against_or(TestType(10, super::index.num_docs(), 128));
}
