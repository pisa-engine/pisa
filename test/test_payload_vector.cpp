#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <algorithm>
#include <string_view>

#include <fmt/format.h>
#include <gsl/span>
#include <rapidcheck.h>

#include "payload_vector.hpp"

using namespace pisa;
using namespace std::literals::string_view_literals;

inline std::byte operator"" _b(unsigned long long n)
{
    return std::byte(n);
}
inline std::byte operator"" _b(char c)
{
    return std::byte(c);
}

TEST_CASE("Unpack head", "[payload_vector][unit]")
{
    std::vector<std::byte> bytes{0_b, 1_b, 2_b, 3_b, 4_b, 5_b};
    REQUIRE(
        unpack_head<std::byte>(bytes)
        == std::tuple(0_b, gsl::make_span(std::vector<std::byte>{1_b, 2_b, 3_b, 4_b, 5_b})));
    auto [b, i, s] = unpack_head<std::byte, uint32_t>(bytes);
    CHECK(b == 0_b);
    CHECK(i == uint32_t(67305985));
    CHECK(s == gsl::make_span(std::vector<std::byte>{5_b}));
    REQUIRE_THROWS_MATCHES(
        (unpack_head<std::byte, uint32_t, uint16_t>(bytes)),
        std::runtime_error,
        Catch::Predicate<std::runtime_error>([](std::runtime_error const& err) -> bool {
            return std::string(err.what())
                == "Cannot unpack span of size 6 into structure of size 7";
        }));
}

TEST_CASE("Split span", "[payload_vector][unit]")
{
    std::vector<std::byte> bytes{0_b, 1_b, 2_b, 3_b, 4_b, 5_b};
    REQUIRE(
        split(bytes, 0)
        == std::tuple(
            gsl::make_span(std::vector<std::byte>{}),
            gsl::make_span(std::vector<std::byte>{0_b, 1_b, 2_b, 3_b, 4_b, 5_b})));
    REQUIRE(
        split(bytes, 4)
        == std::tuple(
            gsl::make_span(std::vector<std::byte>{0_b, 1_b, 2_b, 3_b}),
            gsl::make_span(std::vector<std::byte>{4_b, 5_b})));
    REQUIRE(
        split(bytes, 6)
        == std::tuple(
            gsl::make_span(std::vector<std::byte>{0_b, 1_b, 2_b, 3_b, 4_b, 5_b}),
            gsl::make_span(std::vector<std::byte>{})));
    REQUIRE_THROWS_MATCHES(
        split(bytes, 7),
        std::runtime_error,
        Catch::Predicate<std::runtime_error>([](std::runtime_error const& err) -> bool {
            return std::string(err.what()) == "Cannot split span of size 6 at position 7";
        }));
}

TEST_CASE("Cast span", "[payload_vector][unit]")
{
    std::vector<std::byte> bytes{0_b, 1_b, 2_b, 3_b, 4_b, 5_b};
    REQUIRE(cast_span<uint16_t>(bytes) == gsl::make_span(std::vector<uint16_t>{256, 770, 1284}));
    REQUIRE_THROWS_MATCHES(
        cast_span<uint32_t>(bytes),
        std::runtime_error,
        Catch::Predicate<std::runtime_error>([](std::runtime_error const& err) -> bool {
            return std::string(err.what()) == "Failed to cast byte-span to span of T of size 4";
        }));
}

TEST_CASE("Test string-payload vector", "[payload_vector][unit]")
{
    std::vector<detail::size_type> offsets{0, 3, 6, 10, 13};
    std::vector<std::byte> payloads{
        'a'_b, 'b'_b, 'c'_b, 'd'_b, 'e'_b, 'f'_b, 'g'_b, 'h'_b, 'i'_b, 'j'_b, 'k'_b, 'l'_b, 'm'_b};
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
        REQUIRE(iter == vec.end());
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
    // clang-format off
    REQUIRE(std::vector<char>(encoded.begin(), encoded.end()) == std::vector<char>{
            /* length */ 4, 0, 0, 0, 0, 0, 0, 0,
            /* offset 0 */ 0, 0, 0, 0, 0, 0, 0, 0,
            /* offset 1 */ 3, 0, 0, 0, 0, 0, 0, 0,
            /* offset 2 */ 6, 0, 0, 0, 0, 0, 0, 0,
            /* offset 3 */ 10, 0, 0, 0, 0, 0, 0, 0,
            /* offset 4 */ 13, 0, 0, 0, 0, 0, 0, 0,
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm'});
    // clang-format on
}

TEST_CASE("Test payload vector decoding", "[payload_vector][unit]")
{
    // clang-format off
    std::vector<char> data{
            /* length */ 4, 0, 0, 0, 0, 0, 0, 0,
            /* offset 0 */ 0, 0, 0, 0, 0, 0, 0, 0,
            /* offset 1 */ 3, 0, 0, 0, 0, 0, 0, 0,
            /* offset 2 */ 6, 0, 0, 0, 0, 0, 0, 0,
            /* offset 3 */ 10, 0, 0, 0, 0, 0, 0, 0,
            /* offset 4 */ 13, 0, 0, 0, 0, 0, 0, 0,
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm'};
    // clang-format on
    auto vec = Payload_Vector<std::string_view>::from(
        gsl::make_span(reinterpret_cast<std::byte const*>(data.data()), data.size()));
    REQUIRE(
        std::vector<std::string>(vec.begin(), vec.end())
        == std::vector<std::string>{"abc", "def", "ghij", "klm"});
}

TEST_CASE("Test binary search", "[payload_vector][unit]")
{
    std::vector<int> elements{0, 1, 2, 4, 5, 7, 8, 100};
    REQUIRE(pisa::binary_search(elements.begin(), elements.end(), 0).value() == 0);
    REQUIRE(pisa::binary_search(elements.begin(), elements.end(), 1).value() == 1);
    REQUIRE(pisa::binary_search(elements.begin(), elements.end(), 2).value() == 2);
    REQUIRE(pisa::binary_search(elements.begin(), elements.end(), 4).value() == 3);
    REQUIRE(pisa::binary_search(elements.begin(), elements.end(), 5).value() == 4);
    REQUIRE(pisa::binary_search(elements.begin(), elements.end(), 7).value() == 5);
    REQUIRE(pisa::binary_search(elements.begin(), elements.end(), 8).value() == 6);
    REQUIRE(pisa::binary_search(elements.begin(), elements.end(), 100).value() == 7);
    REQUIRE(pisa::binary_search(elements.begin(), elements.end(), 101).has_value() == false);
}

TEST_CASE("Binary search for sorted values is correct", "[payload_vector][prop]")
{
    rc::check([](std::vector<int> elements, std::vector<int> lookups) {
        std::sort(elements.begin(), elements.end());
        for (auto v: lookups) {
            if (auto pos = pisa::binary_search(elements, v); pos) {
                REQUIRE(*pos >= 0);
                REQUIRE(*pos < elements.size());
                REQUIRE(elements[*pos] == v);
            }
        }
    });
}

TEST_CASE("Binary search for unsorted values doesn't crash", "[payload_vector][prop]")
{
    rc::check([](std::vector<int> elements, std::vector<int> lookups) {
        for (auto v: lookups) {
            pisa::binary_search(elements, 0);
        }
    });
}
