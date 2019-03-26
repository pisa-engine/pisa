#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <algorithm>
#include <string_view>

#include <gsl/span>

#include "payload_vector.hpp"

using namespace pisa;
using namespace std::literals::string_view_literals;

TEST_CASE("Test string-payload vector", "[payload_vector][unit]")
{
    std::vector<detail::size_type> offsets{0, 3, 6, 10, 13};
    std::vector<std::byte> payloads{std::byte{'a'},
                                    std::byte{'b'},
                                    std::byte{'c'},
                                    std::byte{'d'},
                                    std::byte{'e'},
                                    std::byte{'f'},
                                    std::byte{'g'},
                                    std::byte{'h'},
                                    std::byte{'i'},
                                    std::byte{'j'},
                                    std::byte{'k'},
                                    std::byte{'l'},
                                    std::byte{'m'}};
    Payload_Vector<std::string_view> vec(gsl::make_span(offsets), gsl::make_span(payloads));
    SECTION("size") { REQUIRE(vec.size() == 4); }
    SECTION("iterator equality")
    {
        REQUIRE(vec.begin() == vec.begin());
        REQUIRE(std::next(vec.begin()) != vec.begin());
        REQUIRE(std::next(vec.begin()) != vec.end());
        REQUIRE(vec.end() == vec.end());
    }
    SECTION("dereference with ++iter")
    {
        auto iter = vec.begin();
        REQUIRE(*iter == "abc"sv);
        ++iter;
        REQUIRE(*iter == "def"sv);
        ++iter;
        REQUIRE(*iter == "ghij"sv);
        ++iter;
        REQUIRE(*iter == "klm"sv);
        ++iter;
        REQUIRE(iter== vec.end());
    }
    SECTION("dereference with iter++")
    {
        auto iter = vec.begin();
        CHECK(*iter++ == "abc"sv);
        CHECK(*iter++ == "def"sv);
        CHECK(*iter++ == "ghij"sv);
        CHECK(*iter++ == "klm"sv);
        CHECK(iter == vec.end());
    }
    SECTION("dereference with begin() + n")
    {
        CHECK(*(vec.begin() + 0) == "abc"sv);
        CHECK(*(vec.begin() + 1) == "def"sv);
        CHECK(*(vec.begin() + 2) == "ghij"sv);
        CHECK(*(vec.begin() + 3) == "klm"sv);
        CHECK(vec.begin() + 4 == vec.end());
    }
    SECTION("dereference with next()")
    {
        CHECK(*std::next(vec.begin(), 0) == "abc"sv);
        CHECK(*std::next(vec.begin(), 1) == "def"sv);
        CHECK(*std::next(vec.begin(), 2) == "ghij"sv);
        CHECK(*std::next(vec.begin(), 3) == "klm"sv);
        CHECK(std::next(vec.begin(), 4) == vec.end());
    }
    SECTION("dereference with begin() - n")
    {
        CHECK(*(vec.end() - 4) == "abc"sv);
        CHECK(*(vec.end() - 3) == "def"sv);
        CHECK(*(vec.end() - 2) == "ghij"sv);
        CHECK(*(vec.end() - 1) == "klm"sv);
        CHECK(vec.end() - 0 == vec.end());
    }
    SECTION("to vector")
    {
        std::vector<std::string_view> v(vec.begin(), vec.end());
        REQUIRE(v == std::vector<std::string_view>{"abc"sv, "def"sv, "ghij"sv, "klm"sv});
    }
    SECTION("operator[]")
    {
        CHECK(vec[0] == "abc"sv);
        CHECK(vec[1] == "def"sv);
        CHECK(vec[2] == "ghij"sv);
        CHECK(vec[3] == "klm"sv);
    }
    SECTION("binary search")
    {
        CHECK(std::lower_bound(vec.begin(), vec.end(), "de"sv) == std::next(vec.begin()));
        CHECK(std::lower_bound(vec.begin(), vec.end(), "def"sv) == std::next(vec.begin()));
        CHECK(std::lower_bound(vec.begin(), vec.end(), "dew"sv) == std::next(vec.begin(), 2));
    }
}

TEST_CASE("Test payload vector container", "[payload_vector][unit]")
{
    std::vector<std::string> vec{"abc", "def", "ghij", "klm"};
    std::ostringstream str;
    auto container = encode_payload_vector(gsl::span<std::string const>(vec));
    Payload_Vector<std::string_view> pvec(container);
    REQUIRE(std::vector<std::string>(vec.begin(), vec.end()) == vec);
}

TEST_CASE("Test payload vector encoding", "[payload_vector][unit]")
{
    std::vector<std::string> vec{"abc", "def", "ghij", "klm"};
    std::ostringstream str;
    encode_payload_vector(gsl::span<std::string const>(vec)).to_stream(str);
    auto encoded = str.str();
    REQUIRE(std::vector<char>(encoded.begin(), encoded.end()) == std::vector<char>{
            /* length */ 4, 0, 0, 0, 0, 0, 0, 0,
            /* offset 0 */ 0, 0, 0, 0, 0, 0, 0, 0,
            /* offset 1 */ 3, 0, 0, 0, 0, 0, 0, 0,
            /* offset 2 */ 6, 0, 0, 0, 0, 0, 0, 0,
            /* offset 3 */ 10, 0, 0, 0, 0, 0, 0, 0,
            /* offset 4 */ 13, 0, 0, 0, 0, 0, 0, 0,
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm'});
}

TEST_CASE("Test payload vector decoding", "[payload_vector][unit]")
{
    std::vector<char> data{
            /* length */ 4, 0, 0, 0, 0, 0, 0, 0,
            /* offset 0 */ 0, 0, 0, 0, 0, 0, 0, 0,
            /* offset 1 */ 3, 0, 0, 0, 0, 0, 0, 0,
            /* offset 2 */ 6, 0, 0, 0, 0, 0, 0, 0,
            /* offset 3 */ 10, 0, 0, 0, 0, 0, 0, 0,
            /* offset 4 */ 13, 0, 0, 0, 0, 0, 0, 0,
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm'};
    auto vec = Payload_Vector<std::string_view>::from(
        gsl::make_span(reinterpret_cast<std::byte const *>(data.data()), data.size()));
    REQUIRE(std::vector<std::string>(vec.begin(), vec.end()) ==
            std::vector<std::string>{"abc", "def", "ghij", "klm"});
}
