// Copyright 2024 PISA developers
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <span>
#include <sstream>
#include <string_view>
#include <vector>

#include "pisa/lookup_table.hpp"
#include "pisa/span.hpp"

using namespace std::literals;

static const std::vector<pisa::lt::v1::Flags> FLAG_COMBINATIONS{
    pisa::lt::v1::Flags(),
    pisa::lt::v1::Flags(pisa::lt::v1::flags::SORTED),
    pisa::lt::v1::Flags(pisa::lt::v1::flags::WIDE_OFFSETS),
    pisa::lt::v1::Flags(pisa::lt::v1::flags::SORTED | pisa::lt::v1::flags::WIDE_OFFSETS)
};

template <typename T>
auto repeat(T value, std::size_t length) -> std::vector<T> {
    std::vector<T> bytes(length);
    std::fill_n(bytes.begin(), length, value);
    return bytes;
}

auto zeroes(std::size_t length) -> std::vector<int> {
    return repeat(0, length);
}

auto vec_of(std::byte val) -> std::vector<std::byte> {
    return {val};
}

auto vec_of(int val) -> std::vector<std::byte> {
    return {static_cast<std::byte>(val)};
}

auto vec_of(std::initializer_list<std::byte> vals) -> std::vector<std::byte> {
    return {vals};
}

auto vec_of(std::vector<int> const& vals) -> std::vector<std::byte> {
    std::vector<std::byte> result;
    std::transform(vals.begin(), vals.end(), std::back_inserter(result), [](int v) {
        return std::byte(v);
    });
    return result;
}

template <typename... Ts>
auto mem(Ts... input) -> std::vector<std::byte> {
    std::vector<std::byte> result;
    (
        [&]() {
            auto v = vec_of(input);
            result.insert(result.end(), v.begin(), v.end());
        }(),
        ...
    );
    return result;
}

auto encode_lookup_table(std::vector<std::string> payloads, pisa::lt::v1::Flags flags) {
    std::ostringstream out;
    pisa::LookupTableEncoder::v1(flags)
        .insert_span(std::span<std::string const>(payloads.data(), payloads.size()))
        .encode(out);
    return out.str();
}

TEST_CASE("flags") {
    SECTION("defaults") {
        auto default_flags = pisa::lt::v1::Flags();
        REQUIRE_FALSE(default_flags.sorted());
        REQUIRE_FALSE(default_flags.wide_offsets());
    }
    SECTION("sorted") {
        auto default_flags = pisa::lt::v1::Flags(pisa::lt::v1::flags::SORTED);
        REQUIRE(default_flags.sorted());
        REQUIRE_FALSE(default_flags.wide_offsets());
    }
    SECTION("wide_offsets") {
        auto default_flags = pisa::lt::v1::Flags(pisa::lt::v1::flags::WIDE_OFFSETS);
        REQUIRE_FALSE(default_flags.sorted());
        REQUIRE(default_flags.wide_offsets());
    }
    SECTION("sorted + wide_offsets") {
        auto default_flags =
            pisa::lt::v1::Flags(pisa::lt::v1::flags::SORTED | pisa::lt::v1::flags::WIDE_OFFSETS);
        REQUIRE(default_flags.sorted());
        REQUIRE(default_flags.wide_offsets());
    }
}

TEST_CASE("LookupTable::from") {
    SECTION("wrong identifier") {
        auto bytes = mem(0, 0, 0, 0);
        REQUIRE_THROWS_WITH(
            pisa::LookupTable::from_bytes(std::span(bytes.data(), bytes.size())),
            "lookup table verification byte invalid: must be 0x87 but 0x0 given"
        );
    }
    SECTION("invalid version 0") {
        auto bytes = mem(0x87, 0, 0, 0);
        REQUIRE_THROWS_WITH(
            pisa::LookupTable::from_bytes(std::span(bytes.data(), bytes.size())),
            "only version 1 is valid but 0 given"
        );
    }
    SECTION("invalid version 2") {
        auto bytes = mem(0x87, 2, 0, 0);
        REQUIRE_THROWS_WITH(
            pisa::LookupTable::from_bytes(std::span(bytes.data(), bytes.size())),
            "only version 1 is valid but 2 given"
        );
    }
    SECTION("padding is invalid") {
        auto bytes = mem(0x87, 1, 0, 0);
        REQUIRE_THROWS_WITH(
            pisa::LookupTable::from_bytes(std::span(bytes.data(), bytes.size())),
            "not enough bytes for header"
        );
        bytes = mem(0x87, 1, 0, 0, 0, 0, 0, 1);
        REQUIRE_THROWS_WITH(
            pisa::LookupTable::from_bytes(std::span(bytes.data(), bytes.size())),
            "bytes 3-7 must be all 0 but are 0x0 0x0 0x0 0x0 0x1"
        );
        bytes = mem(0x87, 1, 0, 1, 2, 3, 4, 5);
        REQUIRE_THROWS_WITH(
            pisa::LookupTable::from_bytes(std::span(bytes.data(), bytes.size())),
            "bytes 3-7 must be all 0 but are 0x1 0x2 0x3 0x4 0x5"
        );
    }
    SECTION("empty table narrow offsets") {
        auto bytes = mem(0x87, 1, zeroes(18));
        auto lt = pisa::LookupTable::from_bytes(std::span(bytes.data(), bytes.size()));
        REQUIRE(lt.size() == 0);
    }
    SECTION("empty table wide offsets") {
        auto bytes = mem(0x87, 1, pisa::lt::v1::flags::WIDE_OFFSETS, zeroes(21));
        auto lt = pisa::LookupTable::from_bytes(std::span(bytes.data(), bytes.size()));
        REQUIRE(lt.size() == 0);
    }
    SECTION("empty table must have a single offset") {
        auto bytes = mem(0x87, 1, zeroes(14));
        REQUIRE_THROWS_WITH(
            pisa::LookupTable::from_bytes(std::span(bytes.data(), bytes.size())),
            "not enough bytes for offsets"
        );
    }
    SECTION("not enough bytes for offsets") {
        auto bytes = mem(0x87, 1, zeroes(6), 1, zeroes(7));
        REQUIRE_THROWS_WITH(
            pisa::LookupTable::from_bytes(std::span(bytes.data(), bytes.size())),
            "not enough bytes for offsets"
        );
    }
    SECTION("12 bytes is not enough for 3 wide offsets") {
        /* clang-format off */
        auto bytes = mem(
            // header
            0x87, 1, pisa::lt::v1::flags::WIDE_OFFSETS, zeroes(5),
            // size
            2, zeroes(7),
            // offsets
            zeroes(12)
        );
        /* clang-format on */
        REQUIRE_THROWS_WITH(
            pisa::LookupTable::from_bytes(std::span(bytes.data(), bytes.size())),
            "not enough bytes for offsets"
        );
    }
    SECTION("12 bytes is enough for 3 narrow offsets") {
        /* clang-format off */
        auto bytes = mem(
            // header
            0x87, 1, 0, zeroes(5),
            // size
            2, zeroes(7),
            // offsets
            zeroes(12)
        );
        /* clang-format on */
        auto lt = pisa::LookupTable::from_bytes(std::span(bytes.data(), bytes.size()));
        REQUIRE(lt.size() == 2);
    }
    SECTION("[a, bcd, efgh] with narrow offsets") {
        /* clang-format off */
        auto bytes = mem(
            // header
            0x87, 1, 0, zeroes(5),
            // size
            3, zeroes(7),
            // offsets
            zeroes(4),
            1, zeroes(3),
            4, zeroes(3),
            8, zeroes(3),
            // payloads
            'a',
            'b', 'c', 'd',
            'e', 'f', 'g', 'h'
        );
        /* clang-format on */
        auto lt = pisa::LookupTable::from_bytes(std::span(bytes.data(), bytes.size()));
        REQUIRE(lt.size() == 3);
        REQUIRE(
            lt[0]
            == std::span<std::byte const>(reinterpret_cast<std::byte const*>(bytes.data()) + 32, 1)
        );
        REQUIRE(
            lt[1]
            == std::span<std::byte const>(reinterpret_cast<std::byte const*>(bytes.data()) + 33, 3)
        );
        REQUIRE(
            lt[2]
            == std::span<std::byte const>(reinterpret_cast<std::byte const*>(bytes.data()) + 36, 4)
        );
    }
}

TEST_CASE("LookupTable v1") {
    SECTION("encode [a, bcd, efgh]") {
        /* clang-format off */
        auto expected = mem(
            // header
            0x87, 1, pisa::lt::v1::flags::WIDE_OFFSETS, zeroes(5),
            // size
            3, zeroes(7),
            // offsets
            zeroes(8),
            1, zeroes(7),
            4, zeroes(7),
            8, zeroes(7),
            // payloads
            'a',
            'b', 'c', 'd',
            'e', 'f', 'g', 'h'
        );
        /* clang-format on */
        std::ostringstream out;
        auto encoder =
            pisa::LookupTableEncoder::v1(pisa::lt::v1::Flags(pisa::lt::v1::flags::WIDE_OFFSETS));
        std::vector<std::string> payloads{"a", "bcd", "efgh"};
        encoder.insert_span(std::span<std::string const>(payloads.data(), payloads.size()));
        encoder.encode(out);
        std::string bytes = out.str();
        auto actual = std::as_bytes(std::span<char>(bytes.data(), bytes.size()));
        REQUIRE(actual == std::as_bytes(std::span(expected.data(), expected.size())));
    }
    SECTION("wrong order in sorted table") {
        std::ostringstream out;
        auto encoder = pisa::LookupTableEncoder::v1(pisa::lt::v1::Flags(pisa::lt::v1::flags::SORTED));
        std::vector<std::string> payloads{"bcd", "a", "efgh"};
        REQUIRE_THROWS_WITH(
            encoder.insert_span(std::span<std::string const>(payloads.data(), payloads.size())),
            "payloads not strictly sorted in sorted table"
        );
    }
    SECTION("detects duplicates") {
        auto flags = GENERATE_REF(from_range(FLAG_COMBINATIONS));
        std::ostringstream out;
        auto encoder = pisa::LookupTableEncoder::v1(flags);
        std::vector<std::string> payloads{"a", "b", "b", "c"};
        REQUIRE_THROWS_WITH(
            encoder.insert_span(std::span<std::string const>(payloads.data(), payloads.size())),
            flags.sorted() ? "payloads not strictly sorted in sorted table" : "payload duplicate"
        );
    }
    SECTION("operator[]") {
        auto flags = GENERATE_REF(from_range(FLAG_COMBINATIONS));
        std::string bytes = encode_lookup_table({"a", "bcd", "efgh"}, flags);

        auto lt = pisa::LookupTable::from_bytes(std::as_bytes(std::span<char>(bytes)));

        REQUIRE(lt.at<std::string_view>(0) == "a");
        REQUIRE(lt.at<std::string_view>(1) == "bcd");
        REQUIRE(lt.at<std::string_view>(2) == "efgh");

        REQUIRE(lt.at<std::string>(0) == "a");
        REQUIRE(lt.at<std::string>(1) == "bcd");
        REQUIRE(lt.at<std::string>(2) == "efgh");

        auto val = lt.at<std::span<char const>>(0);
        REQUIRE(std::vector(val.begin(), val.end()) == std::vector<char>{'a'});
        val = lt.at<std::span<char const>>(1);
        REQUIRE(std::vector(val.begin(), val.end()) == std::vector<char>{'b', 'c', 'd'});
        val = lt.at<std::span<char const>>(2);
        REQUIRE(std::vector(val.begin(), val.end()) == std::vector<char>{'e', 'f', 'g', 'h'});
    }
    SECTION("find()") {
        auto flags = GENERATE_REF(from_range(FLAG_COMBINATIONS));
        std::string bytes = encode_lookup_table({"a", "bcd", "efgh"}, flags);
        auto lt = pisa::LookupTable::from_bytes(std::as_bytes(std::span<char>(bytes)));

        REQUIRE_FALSE(lt.find(""sv).has_value());
        REQUIRE(lt.find("a"sv) == 0);
        REQUIRE_FALSE(lt.find("aa"sv).has_value());
        REQUIRE(lt.find("bcd"sv) == 1);
        REQUIRE_FALSE(lt.find("bcde"sv).has_value());
        REQUIRE(lt.find("efgh"sv) == 2);
        REQUIRE_FALSE(lt.find("efghi"sv).has_value());
    }
}
