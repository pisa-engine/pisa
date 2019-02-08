#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <range/v3/to_container.hpp>

#include "test_common.hpp"

#include "accumulator/lazy_accumulator.hpp"
#include "ds2i_config.hpp"
#include "index_types.hpp"
#include "query/queries.hpp"
#include "query/scored_range.hpp"

namespace pisa { namespace test {

    template<typename Index>
    struct index_initialization {

        using index_type = Index;
        using WandType = wand_data<bm25, wand_data_raw<bm25>>;
        index_initialization()
            : collection(DS2I_SOURCE_DIR "/test/test_data/test_collection")
            , document_sizes(DS2I_SOURCE_DIR "/test/test_data/test_collection.sizes")
            , wdata(document_sizes.begin()->begin(), collection.num_docs(), collection)
        {
            typename index_type::builder builder(collection.num_docs(), params);
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
            ranked_or_query<index_type, WandType> or_q(index, wdata, 10);

            for (auto const& q: queries) {
                or_q(q);
                op_q(q);
                REQUIRE(or_q.topk().size() == op_q.topk().size());
                for (size_t i = 0; i < or_q.topk().size(); ++i) {
                    REQUIRE(or_q.topk()[i].first ==
                            Approx(op_q.topk()[i].first).epsilon(0.1)); // tolerance is % relative
                }
            }
        }

        template <typename QueryOp>
        void test_with_ranges(QueryOp &op_q) const
        {
            ranked_or_taat_query<index_type, WandType> or_q(index, wdata, 10);

            for (auto const &q : queries) {
                or_q(q);
                op_q(gsl::make_span(max_scored_ranges(index, wdata, q)));
                REQUIRE(or_q.topk().size() == op_q.topk().size());
                for (size_t i = 0; i < or_q.topk().size(); ++i) {
                    REQUIRE(or_q.topk()[i].first ==
                            Approx(op_q.topk()[i].first).epsilon(0.1)); // tolerance is % relative
                }
            }
        }

        void test_k_size() const
        {
            ranked_or_query<index_type, WandType> or_10(index, wdata, 10);
            ranked_or_query<index_type, WandType> or_1(index, wdata, 1);

            for (auto const &q : queries) {
                or_10(q);
                or_1(q);
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

TEST_CASE_METHOD(pisa::test::index_initialization<single_index>, "wand")
{
    pisa::wand_query<index_type, WandType> wand_q(index, wdata, 10);
    test_against_or(wand_q);
}

TEST_CASE_METHOD(pisa::test::index_initialization<single_index>, "maxscore")
{
    pisa::maxscore_query<index_type, WandType> maxscore_q(index, wdata, 10);
    test_against_or(maxscore_q);
}

TEST_CASE_METHOD(pisa::test::index_initialization<single_index>, "block_max_maxscore")
{
    pisa::block_max_maxscore_query<index_type, WandType> bmm_q(index, wdata, 10);
    test_against_or(bmm_q);
}

TEST_CASE_METHOD(pisa::test::index_initialization<single_index>, "ranked_or_taat")
{

    pisa::ranked_or_taat_query<index_type, WandType, pisa::Simple_Accumulator> ranked_or_taat_q(
        index, wdata, 10);
    test_against_or(ranked_or_taat_q);
}

TEST_CASE_METHOD(pisa::test::index_initialization<single_index>, "ranked_or_taat_lazy")
{
    pisa::ranked_or_taat_query<index_type, WandType, pisa::Lazy_Accumulator<8>> ranked_or_taat_q(
        index, wdata, 10);
    test_against_or(ranked_or_taat_q);
}

/// Issue #26 https://github.com/pisa-engine/pisa/issues/26
TEST_CASE_METHOD(pisa::test::index_initialization<single_index>, "topk_size_ranked_or")
{
    test_k_size();
}

/// Range-based

TEST_CASE_METHOD(pisa::test::index_initialization<single_index>,
                 "single ranked_or_taat_ranges",
                 "[.]")
{
    pisa::ranked_or_taat_query<index_type, WandType, pisa::Simple_Accumulator> ranked_or_taat_q(
        index, wdata, 10);
    test_with_ranges(ranked_or_taat_q);
}

TEST_CASE_METHOD(pisa::test::index_initialization<block_simdbp_index>,
                 "block ranked_or_taat_ranges")
{
    pisa::ranked_or_taat_query<index_type, WandType, pisa::Simple_Accumulator> ranked_or_taat_q(
        index, wdata, 10);
    test_with_ranges(ranked_or_taat_q);
}

TEST_CASE_METHOD(pisa::test::index_initialization<single_index>, "single wand_ranges", "[.]")
{
    pisa::wand_query<index_type, WandType> wand_q(index, wdata, 10);
    test_with_ranges(wand_q);
}

TEST_CASE_METHOD(pisa::test::index_initialization<block_simdbp_index>, "block wand_ranges", "[.]")
{
    pisa::wand_query<index_type, WandType> wand_q(index, wdata, 10);
    test_with_ranges(wand_q);
}

TEST_CASE_METHOD(pisa::test::index_initialization<single_index>, "single ranked_or_ranges", "[.]")
{
    pisa::ranked_or_query<index_type, WandType> ranked_or_q(index, wdata, 10);
    test_with_ranges(ranked_or_q);
}

TEST_CASE_METHOD(pisa::test::index_initialization<block_simdbp_index>, "block ranked_or_ranges")
{
    pisa::ranked_or_query<index_type, WandType> ranked_or_q(index, wdata, 10);
    test_with_ranges(ranked_or_q);
}

TEST_CASE_METHOD(pisa::test::index_initialization<single_index>, "single maxscore_ranges", "[.]")
{
    pisa::maxscore_query<index_type, WandType> maxscore_q(index, wdata, 10);
    test_with_ranges(maxscore_q);
}

TEST_CASE_METHOD(pisa::test::index_initialization<block_simdbp_index>,
                 "block maxscore_ranges",
                 "[.]")
{
    pisa::maxscore_query<index_type, WandType> maxscore_q(index, wdata, 10);
    test_with_ranges(maxscore_q);
}

/// DRAAT

TEST_CASE_METHOD(pisa::test::index_initialization<single_index>, "single draat_taat", "[.]")
{
    auto q = pisa::draat_query(
        pisa::ranked_or_taat_query<index_type, WandType, pisa::Simple_Accumulator>(
            index, wdata, 10),
        1003,
        index.num_docs(),
        10);
    test_with_ranges(q);
}

TEST_CASE_METHOD(pisa::test::index_initialization<single_index>, "single draat_wand", "[.]")
{
    auto q = pisa::draat_query(
        pisa::wand_query<index_type, WandType>(index, wdata, 10), 1003, index.num_docs(), 10);
    test_with_ranges(q);
}

TEST_CASE_METHOD(pisa::test::index_initialization<single_index>, "single draat_ranked_or", "[.]")
{
    auto q = pisa::draat_query(
        pisa::ranked_or_query<index_type, WandType>(index, wdata, 10), 1003, index.num_docs(), 10);
    test_with_ranges(q);
}

TEST_CASE_METHOD(pisa::test::index_initialization<single_index>,
                 "single draat maxscore_ranges",
                 "[.]")
{
    auto q = draat_query(
        pisa::maxscore_query<index_type, WandType>(index, wdata, 10), 1003, index.num_docs(), 10);
    test_with_ranges(q);
}

/// Block index

TEST_CASE_METHOD(pisa::test::index_initialization<block_simdbp_index>, "block draat taat")
{
    auto q = pisa::draat_query(
        pisa::ranked_or_taat_query<index_type, WandType, pisa::Simple_Accumulator>(
            index, wdata, 10),
        1003,
        index.num_docs(),
        10);
    test_with_ranges(q);
}

TEST_CASE_METHOD(pisa::test::index_initialization<block_simdbp_index>, "block draat wand")
{
    auto q = pisa::draat_query(
        pisa::wand_query<index_type, WandType>(index, wdata, 10), 1003, index.num_docs(), 10);
    test_with_ranges(q);
}

TEST_CASE_METHOD(pisa::test::index_initialization<block_simdbp_index>, "block draat ranked_or")
{
    auto q = pisa::draat_query(
        pisa::ranked_or_query<index_type, WandType>(index, wdata, 10), 1003, index.num_docs(), 10);
    test_with_ranges(q);
}

TEST_CASE_METHOD(pisa::test::index_initialization<block_simdbp_index>, "block draat maxscore")
{
    auto q = pisa::draat_query(
        pisa::maxscore_query<index_type, WandType>(index, wdata, 10), 1003, index.num_docs(), 10);
    test_with_ranges(q);
}
