#define CATCH_CONFIG_MAIN

#include <catch2/catch.hpp>
#include <functional>

#include <boost/iterator/filter_iterator.hpp>
#include <boost/spirit/include/lex_lexertl.hpp>

#include "query/queries.hpp"
#include "tokenizer.hpp"
#include "temporary_directory.hpp"

using namespace pisa;

TEST_CASE("TermTokenizer")
{
    std::string str("w0rd, token-izer. pup's, U.S.a., us.");
    TermTokenizer tokenizer(str);
    REQUIRE(std::vector<std::string>(tokenizer.begin(), tokenizer.end()) ==
            std::vector<std::string>{"w0rd", "token", "izer", "pup's", "U.S.a.", "us"});
}

TEST_CASE("Parse query") {
    Temporary_Directory tmpdir;
    auto lexfile = tmpdir.path() / "lex";
    {
        std::ofstream os(lexfile.string());
        os << "obama\nterm2\ntree\nu.s.a.\nlol's";
    }

    auto [query, id, parsed] =
        GENERATE(table<std::string, std::optional<std::string>, std::vector<term_id_type>>(
            {{"17:obama family tree", "17", {0, 2}},
             {"obama family tree", std::nullopt, {0, 2}},
             {"obama, family, trees", std::nullopt, {0, 2}},
             {"obama + family + tree", std::nullopt, {0, 2}},
             {"lol's", std::nullopt, {4}},
             {"U.S.A.!?", std::nullopt, {3}}}));
    TermProcessor process_term =
        query::term_processor(std::make_optional(lexfile.string()), "krovetz");
    auto q = parse_query(query, process_term);
    REQUIRE(q.id == id);
    REQUIRE(q.terms == parsed);
}
