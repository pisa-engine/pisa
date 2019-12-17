#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <string>

#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>
#include <gsl/span>
#include <rapidcheck.h>
#include <tbb/task_scheduler_init.h>

#include "v1/query.hpp"

using pisa::v1::Query;
using pisa::v1::TermId;

TEST_CASE("Parse query from JSON", "[v1][unit]")
{
    REQUIRE_THROWS(Query::from_json("{}"));
    REQUIRE(Query::from_json("{\"query\": \"tell your dog I said hi\"}").get_raw()
            == "tell your dog I said hi");
    REQUIRE(Query::from_json("{\"term_ids\": [0, 32, 4]}").get_term_ids()
            == std::vector<TermId>{0, 4, 32});
    REQUIRE(Query::from_json("{\"term_ids\": [0, 32, 4]}").k() == 1000);
    auto query = Query::from_json(
        R"({"id": "Q0", "query": "send dog pics", "term_ids": [0, 32, 4], "k": 15, )"
        R"("threshold": 40.5, "selections": )"
        R"({ "unigrams": [0, 2], "bigrams": [[0, 2], [2, 1]]}})");
    REQUIRE(query.get_id() == "Q0");
    REQUIRE(query.k() == 15);
    REQUIRE(query.get_term_ids() == std::vector<TermId>{0, 4, 32});
    REQUIRE(query.get_threshold() == 40.5);
    REQUIRE(query.get_raw() == "send dog pics");
    REQUIRE(query.get_selections().unigrams == std::vector<TermId>{0, 4});
    REQUIRE(query.get_selections().bigrams
            == std::vector<std::pair<TermId, TermId>>{{0, 4}, {4, 32}});
    REQUIRE_THROWS(Query::from_json(
        R"({"id": "Q0", "query": "send dog pics", "term_ids": [0, 32, 4], "k": 15, )"
        R"("threshold": 40.5, "selections": )"
        R"({ "unigrams": [0, 4], "bigrams": [[0, 4], [4, 5]]}})"));
}
