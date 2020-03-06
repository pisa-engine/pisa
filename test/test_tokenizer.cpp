#define CATCH_CONFIG_MAIN

#include <catch2/catch.hpp>
#include <functional>

#include <boost/iterator/filter_iterator.hpp>
#include <boost/spirit/include/lex_lexertl.hpp>
#include <gsl/span>

#include "payload_vector.hpp"
#include "query/queries.hpp"
#include "temporary_directory.hpp"
#include "tokenizer.hpp"

using namespace pisa;

TEST_CASE("TermTokenizer")
{
    std::string str("a 1 12 w0rd, token-izer. pup's, U.S.a., us., hel.lo");
    TermTokenizer tokenizer(str);
    REQUIRE(
        std::vector<std::string>(tokenizer.begin(), tokenizer.end())
        == std::vector<std::string>{
            "a", "1", "12", "w0rd", "token", "izer", "pup", "USa", "us", "hel", "lo"});
}

TEST_CASE("Parse query terms to ids")
{
    Temporary_Directory tmpdir;
    auto lexfile = tmpdir.path() / "lex";
    encode_payload_vector(
        gsl::make_span(std::vector<std::string>{"lol", "obama", "term2", "tree", "usa"}))
        .to_file(lexfile.string());

    auto [query, id, parsed] =
        GENERATE(table<std::string, std::optional<std::string>, std::vector<term_id_type>>(
            {{"17:obama family tree", "17", {1, 3}},
             {"obama family tree", std::nullopt, {1, 3}},
             {"obama, family, trees", std::nullopt, {1, 3}},
             {"obama + family + tree", std::nullopt, {1, 3}},
             {"lol's", std::nullopt, {0}},
             {"U.S.A.!?", std::nullopt, {4}}}));
    CAPTURE(query);
    TermProcessor term_processor(std::make_optional(lexfile.string()), std::nullopt, "krovetz");
    auto q = parse_query_terms(query, term_processor);
    REQUIRE(q.id == id);
    REQUIRE(q.terms == parsed);
}
