#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "test_generic_sequence.hpp"

#include "forward_index.hpp"

#include <vector>

TEST_CASE("write_and_read")
{
    // given
    using namespace pisa;
    std::string invind_input("test_data/test_collection");
    std::string fwdind_file("temp_collection");
    auto fwd = forward_index::from_inverted_index(invind_input, 0, true);

    // when
    forward_index::write(fwd, fwdind_file);
    auto fwd_read = forward_index::read(fwdind_file);

    // then
    REQUIRE(fwd.size() == fwd_read.size());
    REQUIRE(fwd.term_count() == fwd_read.term_count());
    for (uint32_t doc = 0; doc < fwd.size(); ++doc) {
        REQUIRE(fwd.term_count(doc) == fwd_read.term_count(doc));
        REQUIRE(std::equal(fwd[doc].begin(), fwd[doc].end(), fwd_read[doc].begin()));
    }
}
