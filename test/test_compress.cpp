#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <fstream>
#include <memory>

#include <fmt/format.h>

#include "forward_index_builder.hpp"
#include "invert.hpp"
#include "parser.hpp"
#include "pisa/compress.hpp"
#include "pisa/scorer/scorer.hpp"
#include "pisa/wand_data.hpp"
#include "pisa_config.hpp"
#include "temporary_directory.hpp"
#include "text_analyzer.hpp"
#include "token_filter.hpp"
#include "tokenizer.hpp"
#include "type_safe.hpp"
#include "wand_utils.hpp"

void build_index(pisa::TemporaryDirectory const& tmp) {
    auto fwd_base_path = (tmp.path() / "tiny.fwd");
    auto inv_base_path = (tmp.path() / "tiny.inv");
    {
        std::ifstream is(PISA_SOURCE_DIR "/test/test_data/tiny/tiny.plaintext");
        pisa::Forward_Index_Builder builder;
        auto analyzer =
            std::make_shared<pisa::TextAnalyzer>(std::make_unique<pisa::EnglishTokenizer>());
        analyzer->emplace_token_filter<pisa::LowercaseFilter>();
        builder.build(
            is, fwd_base_path.string(), pisa::record_parser("plaintext", is), analyzer, 10, 2
        );
    }
    pisa::invert::invert_forward_index(fwd_base_path.string(), inv_base_path.string(), {});
}

TEST_CASE("Compress index", "[index][compress]") {
    pisa::TemporaryDirectory tmp;
    build_index(tmp);

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
        tmp.path() / "tiny.inv",
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
    pisa::TemporaryDirectory tmp;
    build_index(tmp);

    std::string scorer = GENERATE("bm25", "qld");
    CAPTURE(scorer);
    auto scorer_params = ScorerParams(scorer);
    auto inv_path = (tmp.path() / "tiny.inv").string();
    auto wand_path = (tmp.path() / fmt::format("tiny.wand.{}", scorer)).string();

    pisa::create_wand_data(
        wand_path,
        (tmp.path() / "tiny.inv").string(),
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
        inv_path,
        wand_path,
        encoding,
        (tmp.path() / encoding).string(),
        scorer_params,
        pisa::Size(8),
        true,  // check=true
        in_memory
    );
}
