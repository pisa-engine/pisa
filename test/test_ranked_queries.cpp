#define BOOST_TEST_MODULE ranked_queries

#include "succinct/test_common.hpp"
#include <boost/test/floating_point_comparison.hpp>

#include "ds2i_config.hpp"
#include "index_types.hpp"
#include "queries.hpp"

namespace ds2i { namespace test {

    struct index_initialization {

        typedef single_index index_type;

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
            std::ifstream qfile("test_data/queries");
            while (read_query(q, qfile)) queries.push_back(q);
        }

        global_parameters params;
        binary_freq_collection collection;
        binary_collection document_sizes;
        index_type index;
        std::vector<term_id_vec> queries;
        wand_data<> wdata;

        template <typename QueryOp>
        void test_against_or(QueryOp& op_q) const
        {
            ranked_or_query or_q(wdata, 10);

            for (auto const& q: queries) {
                or_q(index, q);
                op_q(index, q);
                BOOST_REQUIRE_EQUAL(or_q.topk().size(), op_q.topk().size());
                for (size_t i = 0; i < or_q.topk().size(); ++i) {
                    BOOST_REQUIRE_CLOSE(or_q.topk()[i], op_q.topk()[i], 0.1); // tolerance is % relative
                }
            }
        }


    };

}}


BOOST_FIXTURE_TEST_CASE(wand,
                        ds2i::test::index_initialization)
{
    ds2i::wand_query wand_q(wdata, 10);
    test_against_or(wand_q);
}

BOOST_FIXTURE_TEST_CASE(maxscore,
                        ds2i::test::index_initialization)
{
    ds2i::maxscore_query maxscore_q(wdata, 10);
    test_against_or(maxscore_q);
}
