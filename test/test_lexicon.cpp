#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <algorithm>
#include <string_view>

#include "lexicon.hpp"

using namespace std::literals::string_view_literals;

[[nodiscard]] auto operator==(pisa::Lexicon_Data const &lhs, pisa::Lexicon_Data const &rhs) -> bool
{
    return lhs.size == rhs.size and lhs.pointers == rhs.pointers and lhs.payloads == rhs.payloads;
}

pisa::Lexicon_Data lexicon_data()
{
    std::vector<std::ptrdiff_t> pointers{0, 3, 9, 17};
    return pisa::Lexicon_Data{
        3,
        std::vector<char>(reinterpret_cast<char *>(&pointers[0]),
                          reinterpret_cast<char *>(&pointers[0]) + 4 * sizeof(std::ptrdiff_t)),
        {'f', 'o', 'o', 'f', 'o', 'o', 'b', 'a', 'r', 'f', 'o', 'o', 't', 'b', 'a', 'l', 'l'}};
}

TEST_CASE("Lexicon_Data from vector", "[lexicon][unit]")
{
    std::vector<std::string> strings{"foo", "foobar", "football"};
    auto dat = pisa::Lexicon_Data(strings.begin(), strings.end());
    auto expected = lexicon_data();
    REQUIRE(dat.size == expected.size);
    REQUIRE(dat.pointers == expected.pointers);
    REQUIRE(dat.payloads == expected.payloads);
}

TEST_CASE("Serialize", "[lexicon][unit]")
{
    std::vector<char> expected{
        3, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,
        3, 0, 0, 0, 0, 0, 0, 0,
        9, 0, 0, 0, 0, 0, 0, 0,
        17, 0, 0, 0, 0, 0, 0, 0,
        'f', 'o', 'o',
        'f', 'o', 'o', 'b', 'a', 'r',
        'f', 'o', 'o', 't', 'b', 'a', 'l', 'l'};

    SECTION("To vector") { REQUIRE(lexicon_data().serialize() == expected); }
    SECTION("To stream") {
        std::ostringstream os;
        REQUIRE(lexicon_data().serialize(os) == 57);
        auto str = os.str();
        REQUIRE(std::vector<char>(str.begin(), str.end()) == expected);
    }
}

TEST_CASE("Parse", "[lexicon][unit]")
{
    auto lexdata = lexicon_data().serialize();
    auto lex = pisa::Lexicon_View::parse(lexdata.data());
    REQUIRE(std::vector<std::string>(lex.begin(), lex.end()) ==
            std::vector<std::string>{"foo", "foobar", "football"});
}

TEST_CASE("Copy constructor and assignment", "[lexicon][unit]")
{
    auto lexdata = lexicon_data();
    pisa::Lexicon_View lex(lexdata.size, lexdata.pointers.data(), lexdata.payloads.data());
    auto lex2 = lex;
    pisa::Lexicon_View lex3;
    lex3 = lex2;
    REQUIRE(std::vector<std::string>(lex.begin(), lex.end()) ==
            std::vector<std::string>{"foo", "foobar", "football"});
    REQUIRE(std::vector<std::string>(lex2.begin(), lex2.end()) ==
            std::vector<std::string>{"foo", "foobar", "football"});
    REQUIRE(std::vector<std::string>(lex3.begin(), lex3.end()) ==
            std::vector<std::string>{"foo", "foobar", "football"});
}

TEST_CASE("Move constructor and assignment", "[lexicon][unit]")
{
    auto lexdata = lexicon_data();
    pisa::Lexicon_View lex(lexdata.size, lexdata.pointers.data(), lexdata.payloads.data());
    auto lex2 = std::move(lex);
    REQUIRE(std::vector<std::string>(lex2.begin(), lex2.end()) ==
            std::vector<std::string>{"foo", "foobar", "football"});
    lex = std::move(lex2);
    REQUIRE(std::vector<std::string>(lex.begin(), lex.end()) ==
            std::vector<std::string>{"foo", "foobar", "football"});
}

TEST_CASE("Size", "[lexicon][unit]")
{
    auto lexdata = lexicon_data();
    pisa::Lexicon_View lex(lexdata.size, lexdata.pointers.data(), lexdata.payloads.data());
    REQUIRE(lex.size() == 3);
    REQUIRE_FALSE(lex.empty());
}

TEST_CASE("Iterating", "[lexicon][unit]")
{
    auto lexdata = lexicon_data();
    pisa::Lexicon_View lex(lexdata.size, lexdata.pointers.data(), lexdata.payloads.data());

    SECTION("Random access")
    {
        REQUIRE(*lex.begin() == "foo"sv);
        REQUIRE(*std::next(lex.begin()) == "foobar"sv);
        REQUIRE(*std::next(lex.begin(), 2) == "football"sv);
        REQUIRE(std::next(lex.begin(), 3) == lex.end());
    }

    SECTION("To vector")
    {
        REQUIRE(std::vector<std::string>(lex.begin(), lex.end()) ==
                std::vector<std::string>{"foo", "foobar", "football"});
    }
}

TEST_CASE("Binary search", "[lexicon][unit]")
{
    auto lexdata = lexicon_data();
    pisa::Lexicon_View lex(lexdata.size, lexdata.pointers.data(), lexdata.payloads.data());

    SECTION("Lower bound")
    {
        REQUIRE(*std::lower_bound(lex.begin(), lex.end(), "acme"sv) == "foo"sv);
        REQUIRE(*std::lower_bound(lex.begin(), lex.end(), "fo"sv) == "foo"sv);
        REQUIRE(*std::lower_bound(lex.begin(), lex.end(), "foo"sv) == "foo"sv);
        REQUIRE(*std::lower_bound(lex.begin(), lex.end(), "foob"sv) == "foobar"sv);
        REQUIRE(*std::lower_bound(lex.begin(), lex.end(), "foobar"sv) == "foobar"sv);
        REQUIRE(*std::lower_bound(lex.begin(), lex.end(), "foobars"sv) == "football"sv);
        REQUIRE(*std::lower_bound(lex.begin(), lex.end(), "foot"sv) == "football"sv);
        REQUIRE(*std::lower_bound(lex.begin(), lex.end(), "fool"sv) == "football"sv);
        REQUIRE(std::lower_bound(lex.begin(), lex.end(), "fox"sv) == lex.end());
    }
}

TEST_CASE("Random accessors", "[lexicon][unit]")
{
    auto lexdata = lexicon_data();
    pisa::Lexicon_View lex(lexdata.size, lexdata.pointers.data(), lexdata.payloads.data());

    SECTION("operator[]")
    {
        REQUIRE(lex[0] == "foo"sv);
        REQUIRE(lex[1] == "foobar"sv);
        REQUIRE(lex[2] == "football"sv);
    }

    SECTION("at()")
    {
        REQUIRE(lex.at(0) == "foo"sv);
        REQUIRE(lex.at(1) == "foobar"sv);
        REQUIRE(lex.at(2) == "football"sv);
        REQUIRE_THROWS_AS(lex.at(3), std::out_of_range);
    }
}
