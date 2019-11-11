#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "binary_freq_collection.hpp"
#include "intersection_index.hpp"
#include "pisa_config.hpp"
#include "temporary_directory.hpp"

TEST_CASE("Test building intersection collection")
{
    Temporary_Directory tmpdir;
    // pisa::binary_freq_collection const collection(PISA_SOURCE_DIR
    // "/test/test_data/test_collection");
    auto output = (tmpdir.path() / "intersection").string();
    pisa::create_intersection_collection(PISA_SOURCE_DIR "/test/test_data/test_collection", output);
    // pisa::base_binary_freq_collection const collection(output.c_str());
}
