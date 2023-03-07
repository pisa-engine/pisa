#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <array>

#include "memory.hpp"

TEST_CASE("bitwise_reinterpret")
{
    GIVEN("4 bytes in an array")
    {
        std::array<std::uint8_t, 4> memory{1, 2, 3, 4};

        WHEN("Reinterpreting as 4-byte int")
        {
            auto value = *pisa::bitwise_reinterpret<std::uint32_t>(memory.data());
            THEN("Equal to 4 bytes reinterpreted as int")
            {
                REQUIRE(value == (4U << 24) + (3U << 16U) + (2U << 8) + 1U);
            }
        }
        WHEN("Reinterpreting 3 first bytes as 4-byte int")
        {
            auto value = *pisa::bitwise_reinterpret<std::uint32_t>(memory.data(), 3);
            THEN("Equal to the first 3 bytes reinterpreted as int")
            {
                REQUIRE(value == (3U << 16) + (2U << 8) + 1U);
            }
        }
        WHEN("Reinterpreting as 2-byte int")
        {
            auto value = *pisa::bitwise_reinterpret<std::uint16_t>(memory.data());
            THEN("Equal to the first 2 bytes reinterpreted as int")
            {
                REQUIRE(value == (2U << 8) + 1U);
            }
        }
    }
    GIVEN("4-byte integer and array")
    {
        std::array<std::uint8_t, 4> memory{0, 0, 0, 0};
        std::uint32_t value = (4U << 24) + (3U << 16U) + (2U << 8) + 1U;

        WHEN("Reinterpreting as 4-byte int and assigning value")
        {
            pisa::bitwise_reinterpret<std::uint32_t>(memory.data()) = value;
            THEN("Array equal to all bytes of the value")
            {
                REQUIRE(memory == std::array<std::uint8_t, 4>{1, 2, 3, 4});
            }
        }
        WHEN("Reinterpreting as 2-byte int and assigning value")
        {
            pisa::bitwise_reinterpret<std::uint16_t>(memory.data()) = value;
            THEN("Array equal to all bytes of the value")
            {
                REQUIRE(memory == std::array<std::uint8_t, 4>{1, 2, 0, 0});
            }
        }
    }
}
