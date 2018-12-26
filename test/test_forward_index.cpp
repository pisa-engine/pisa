#define BOOST_TEST_MODULE forward_index

#include "test_generic_sequence.hpp"

#include "forward_index.hpp"

#include <vector>

BOOST_AUTO_TEST_CASE(write_and_read)
{
    // given
    using namespace ds2i;
    std::string invind_input("test_data/test_collection");
    std::string fwdind_file("temp_collection");
    auto fwd = forward_index::from_inverted_index(invind_input, 0, true);

    // when
    forward_index::write(fwd, fwdind_file);
    auto fwd_read = forward_index::read(fwdind_file);

    // then
    BOOST_REQUIRE_EQUAL(fwd.size(), fwd_read.size());
    BOOST_REQUIRE_EQUAL(fwd.term_count(), fwd_read.term_count());
    for (uint32_t doc = 0; doc < fwd.size(); ++doc) {
        BOOST_REQUIRE_EQUAL(fwd.term_count(doc), fwd_read.term_count(doc));
        BOOST_CHECK_EQUAL_COLLECTIONS(fwd[doc].begin(),
                                      fwd[doc].end(),
                                      fwd_read[doc].begin(),
                                      fwd_read[doc].end());
    }
}
