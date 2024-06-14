#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "pisa/compress.hpp"
#include "pisa/scorer/scorer.hpp"
#include "pisa/wand_data.hpp"
#include "pisa_config.hpp"
#include "temporary_directory.hpp"
#include "type_safe.hpp"
#include "wand_utils.hpp"

TEST_CASE("Compress index", "[index][compress]") {
    std::string encoding = GENERATE(
        "ef",
        "single",
        "pefuniform",
        "pefopt",
        "block_optpfor",
        "block_varintg8iu",
        "block_streamvbyte",
        "block_maskedvbyte",
        "block_varintgb",
        "block_interpolative",
        "block_qmx",
        "block_simple8b",
        "block_simple16",
        "block_simdbp"
    );
    CAPTURE(encoding);
    bool in_memory = GENERATE(true, false);
    CAPTURE(in_memory);

    pisa::TemporaryDirectory tmp;
    pisa::compress(
        PISA_SOURCE_DIR "/test/test_data/test_collection",
        std::nullopt,  // no wand
        encoding,
        (tmp.path() / encoding).string(),
        ScorerParams(""),  // no scorer
        std::nullopt,  // no quantization
        true,  // check=true
        in_memory
    );
}

TEST_CASE("Compress quantized index", "[index][compress]") {
    auto input = PISA_SOURCE_DIR "/test/test_data/test_collection";

    std::string scorer = GENERATE("bm25", "qld");
    CAPTURE(scorer);
    auto scorer_params = ScorerParams(scorer);

    pisa::TemporaryDirectory tmp;
    pisa::create_wand_data(
        (tmp.path() / "wand").string(),
        input,
        pisa::FixedBlock(64),
        scorer_params,
        false,
        false,
        pisa::Size(8),
        std::unordered_set<std::size_t>()
    );

    std::string encoding = GENERATE(
        "ef",
        "single",
        "pefuniform",
        "pefopt",
        "block_optpfor",
        "block_varintg8iu",
        "block_streamvbyte",
        "block_maskedvbyte",
        "block_varintgb",
        "block_interpolative",
        "block_qmx",
        "block_simple8b",
        "block_simple16",
        "block_simdbp"
    );
    CAPTURE(encoding);
    bool in_memory = GENERATE(true, false);
    CAPTURE(in_memory);

    pisa::compress(
        input,
        (tmp.path() / "wand").string(),
        encoding,
        (tmp.path() / encoding).string(),
        scorer_params,
        pisa::Size(8),
        true,  // check=true
        in_memory
    );
}
