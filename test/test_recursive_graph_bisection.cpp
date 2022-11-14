#define CATCH_CONFIG_MAIN

#include <catch2/catch.hpp>

#include "pisa/forward_index_builder.hpp"
#include "pisa/invert.hpp"
#include "pisa/parser.hpp"
#include "pisa/reorder_docids.hpp"
#include "pisa/temporary_directory.hpp"
#include "pisa_config.hpp"

using namespace pisa;

using StrColl = std::vector<std::vector<std::pair<std::string, std::uint32_t>>>;

[[nodiscard]] auto coll_to_strings(std::string const& coll_file, std::string const& doclex_file)
    -> StrColl
{
    auto doclex_buf = Payload_Vector_Buffer::from_file(doclex_file);
    pisa::Payload_Vector<> doclex(doclex_buf);
    pisa::binary_freq_collection coll(coll_file.c_str());
    StrColl strcoll;
    for (auto posting_list: coll) {
        std::vector<std::pair<std::string, std::uint32_t>> pl;
        std::transform(
            posting_list.docs.begin(),
            posting_list.docs.end(),
            posting_list.freqs.begin(),
            std::back_inserter(pl),
            [&doclex](auto&& doc, auto&& freq) {
                return std::pair<std::string, std::uint32_t>(doclex[doc], freq);
            });
        std::sort(pl.begin(), pl.end());
        strcoll.push_back(pl);
    }
    return strcoll;
}

void compare_strcolls(StrColl const& expected, StrColl const& actual)
{
    REQUIRE(expected.size() == actual.size());
    for (int list_idx = 0; list_idx < expected.size(); list_idx += 1) {
        REQUIRE(expected[list_idx].size() == actual[list_idx].size());
        for (int posting_idx = 0; posting_idx < expected[list_idx].size(); posting_idx += 1) {
            REQUIRE(expected[list_idx][posting_idx].first == actual[list_idx][posting_idx].first);
            REQUIRE(expected[list_idx][posting_idx].second == actual[list_idx][posting_idx].second);
        }
    }
}

TEST_CASE("Reorder documents with BP")
{
    pisa::TemporaryDirectory tmp;

    auto next_record = [](std::istream& in) -> std::optional<Document_Record> {
        Plaintext_Record record;
        if (in >> record) {
            return Document_Record(record.trecid(), record.content(), record.url());
        }
        return std::nullopt;
    };
    auto id = [] {
        return [](std::string&& term) -> std::string { return std::forward<std::string>(term); };
    };

    auto fwd_path = (tmp.path() / "fwd").string();
    auto inv_path = (tmp.path() / "inv").string();
    auto bp_fwd_path = (tmp.path() / "fwd.bp").string();
    auto bp_inv_path = (tmp.path() / "inv.bp").string();

    GIVEN("Built a forward index and inverted")
    {
        std::string collection_input(PISA_SOURCE_DIR "/test/test_data/clueweb1k.plaintext");
        REQUIRE(boost::filesystem::exists(boost::filesystem::path(collection_input)) == true);
        int thread_count = 2;
        int batch_size = 1000;
        pisa::invert::InvertParams params;
        params.num_threads = thread_count;
        params.batch_size = batch_size;

        std::ifstream is(collection_input);
        Forward_Index_Builder builder;
        builder.build(
            is, fwd_path, next_record, id, parse_plaintext_content, batch_size, thread_count);

        pisa::invert::invert_forward_index(fwd_path, inv_path, params);

        WHEN("Reordered documents with BP")
        {
            auto cache_depth = GENERATE(
                std::optional<std::size_t>{},
                std::make_optional<std::size_t>(1),
                std::make_optional<std::size_t>(2));
            int code = recursive_graph_bisection(RecursiveGraphBisectionOptions{
                .input_basename = inv_path,
                .output_basename = bp_inv_path,
                .output_fwd = std::nullopt,
                .input_fwd = std::nullopt,
                .document_lexicon = fmt::format("{}.doclex", fwd_path),
                .reordered_document_lexicon = fmt::format("{}.doclex", bp_fwd_path),
                .depth = cache_depth,
                .node_config = std::nullopt,
                .min_length = 0,
                .compress_fwd = false,
                .print_args = false,
            });
            REQUIRE(code == 0);
            THEN("Both collections are equal when mapped to strings")
            {
                auto expected = coll_to_strings(inv_path, fmt::format("{}.doclex", fwd_path));
                auto actual = coll_to_strings(bp_inv_path, fmt::format("{}.doclex", bp_fwd_path));
                compare_strcolls(expected, actual);
            }
        }

        WHEN("Reordered documents with BP node version")
        {
            int code = recursive_graph_bisection(RecursiveGraphBisectionOptions{
                .input_basename = inv_path,
                .output_basename = bp_inv_path,
                .output_fwd = std::nullopt,
                .input_fwd = std::nullopt,
                .document_lexicon = fmt::format("{}.doclex", fwd_path),
                .reordered_document_lexicon = fmt::format("{}.doclex", bp_fwd_path),
                .depth = std::nullopt,
                .node_config = PISA_SOURCE_DIR "/test/test_data/bp-node-config.txt",
                .min_length = 0,
                .compress_fwd = false,
                .print_args = false,
            });
            REQUIRE(code == 0);
            THEN("Both collections are equal when mapped to strings")
            {
                auto expected = coll_to_strings(inv_path, fmt::format("{}.doclex", fwd_path));
                auto actual = coll_to_strings(bp_inv_path, fmt::format("{}.doclex", bp_fwd_path));
                compare_strcolls(expected, actual);
            }
        }
    }
}
