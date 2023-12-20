#define CATCH_CONFIG_MAIN

#include <catch2/catch.hpp>

#include "pisa/cow_string.hpp"

using namespace pisa;

constexpr char const* VALUE =
    "This is a long enough string so that when in std::string, it is allocated, "
    "and short-string optimization is not used.";

TEST_CASE("CowString") {
    GIVEN("An owned CowString") {
        std::string value = VALUE;
        CowString cow(value);

        WHEN("Accessed with as_view") {
            auto view = cow.as_view();
            THEN("Equal to original value") {
                REQUIRE(view == value);
            }
            THEN("Data location is different") {
                REQUIRE(view.data() != value.data());
            }
        }

        WHEN("Accessed with to_owned") {
            auto owned = std::move(cow).to_owned();
            THEN("Equal to original value") {
                REQUIRE(owned == value);
            }
            THEN("Data location is different") {
                REQUIRE(owned.data() != value.data());
            }
        }
    }
    GIVEN("An owned CowString (moved from value)") {
        std::string value = VALUE;
        // We will check that we never copy the value, thus the data pointer is the same
        char const* data_ptr = value.data();
        CowString cow(std::move(value));

        WHEN("Accessed with as_view") {
            auto view = cow.as_view();
            THEN("Equal to original value") {
                REQUIRE(view == VALUE);
            }
            THEN("Data location is the same as initially") {
                REQUIRE(view.data() == data_ptr);
            }
        }

        WHEN("Accessed with to_owned") {
            auto owned = std::move(cow).to_owned();
            THEN("Equal to original value") {
                REQUIRE(owned == VALUE);
            }
            THEN("Data location is the same as initially") {
                REQUIRE(owned.data() == data_ptr);
            }
        }
    }
    GIVEN("A borrowed CowString") {
        std::string value = VALUE;
        // We will check that we never copy the value, thus the data pointer is the same
        char const* data_ptr = value.data();
        CowString cow(std::string_view{value});

        WHEN("Accessed with as_view") {
            auto view = cow.as_view();
            THEN("Equal to original value") {
                REQUIRE(view == VALUE);
            }
            THEN("Data location is the same as initially") {
                REQUIRE(view.data() == data_ptr);
            }
        }

        WHEN("Accessed with to_owned") {
            auto owned = std::move(cow).to_owned();
            THEN("Equal to original value") {
                REQUIRE(owned == VALUE);
            }
            THEN("Data location is the same as initially") {
                REQUIRE(owned.data() != data_ptr);
            }
        }
    }
}
