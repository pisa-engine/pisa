#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <cstdio>
#include <string>

#include <boost/filesystem.hpp>
#include <gsl/span>
#include <range/v3/view/iota.hpp>

#include "binary_collection.hpp"
#include "filesystem.hpp"
#include "invert.hpp"
#include "payload_vector.hpp"
#include "pisa_config.hpp"
#include "temporary_directory.hpp"

using namespace boost::filesystem;
using namespace pisa;
using namespace pisa::literals;

TEST_CASE("Map sequence of document terms to sequence of postings", "[invert][unit]")
{
    std::vector<std::vector<Term_Id>> documents = {{0_t, 1_t, 2_t, 3_t}, {1_t, 2_t, 3_t, 8_t}};
    std::vector<gsl::span<Term_Id const>> spans = {
        gsl::make_span(documents[0]), gsl::make_span(documents[1])};

    auto postings =
        invert::map_to_postings(invert::ForwardIndexSlice{spans, ranges::views::iota(0_d, 2_d)});
    REQUIRE(
        postings
        == std::vector<std::pair<Term_Id, Document_Id>>{
            {0_t, 0_d}, {1_t, 0_d}, {2_t, 0_d}, {3_t, 0_d}, {1_t, 1_d}, {2_t, 1_d}, {3_t, 1_d}, {8_t, 1_d}});
}

TEST_CASE("Join term from one index to the same term from another", "[invert][unit]")
{
    SECTION("Disjoint")
    {
        std::vector<Document_Id> lower_doc{0_d, 3_d, 5_d};
        std::vector<Frequency> lower_freq{3_f, 4_f, 5_f};
        std::vector<Document_Id> higher_doc{6_d, 7_d, 9_d};
        std::vector<Frequency> higher_freq{6_f, 7_f, 8_f};
        invert::join_term(lower_doc, lower_freq, higher_doc, higher_freq);
        REQUIRE(lower_doc == std::vector<Document_Id>{0_d, 3_d, 5_d, 6_d, 7_d, 9_d});
        REQUIRE(lower_freq == std::vector<Frequency>{3_f, 4_f, 5_f, 6_f, 7_f, 8_f});
    }
    SECTION("With an overlaping document")
    {
        std::vector<Document_Id> lower_doc{0_d, 3_d, 5_d};
        std::vector<Frequency> lower_freq{3_f, 4_f, 5_f};
        std::vector<Document_Id> higher_doc{5_d, 7_d, 9_d};
        std::vector<Frequency> higher_freq{6_f, 7_f, 8_f};
        invert::join_term(lower_doc, lower_freq, higher_doc, higher_freq);
        REQUIRE(lower_doc == std::vector<Document_Id>{0_d, 3_d, 5_d, 7_d, 9_d});
        REQUIRE(lower_freq == std::vector<Frequency>{3_f, 4_f, 11_f, 7_f, 8_f});
    }
}

TEST_CASE("Accumulate postings to Inverted_Index", "[invert][unit]")
{
    std::vector<std::pair<Term_Id, Document_Id>> postings = {{0_t, 0_d},
                                                             {0_t, 1_d},
                                                             {0_t, 2_d},
                                                             {1_t, 0_d},
                                                             {1_t, 0_d},
                                                             {1_t, 0_d},
                                                             {1_t, 0_d},
                                                             {1_t, 1_d},
                                                             {2_t, 5_d}};
    using iterator_type = decltype(postings.begin());
    invert::Inverted_Index index;
    index(tbb::blocked_range<iterator_type>(postings.begin(), postings.end()));
    REQUIRE(
        index.documents
        == std::unordered_map<Term_Id, std::vector<Document_Id>>{
            {0_t, {0_d, 1_d, 2_d}}, {1_t, {0_d, 1_d}}, {2_t, {5_d}}});
    REQUIRE(
        index.frequencies
        == std::unordered_map<Term_Id, std::vector<Frequency>>{
            {0_t, {1_f, 1_f, 1_f}}, {1_t, {4_f, 1_f}}, {2_t, {1_f}}});
}

TEST_CASE("Accumulate postings to Inverted_Index one by one", "[invert][unit]")
{
    std::vector<std::pair<Term_Id, Document_Id>> postings = {
        {0_t, 0_d}, {0_t, 0_d}, {0_t, 1_d}, {0_t, 4_d}, {1_t, 2_d}, {1_t, 4_d}, {2_t, 0_d},
        {2_t, 1_d}, {3_t, 0_d}, {3_t, 1_d}, {3_t, 4_d}, {4_t, 1_d}, {4_t, 1_d}, {4_t, 4_d},
        {5_t, 1_d}, {5_t, 1_d}, {5_t, 2_d}, {5_t, 3_d}, {5_t, 4_d}, {6_t, 1_d}, {6_t, 4_d},
        {6_t, 4_d}, {6_t, 4_d}, {6_t, 4_d}, {7_t, 1_d}, {8_t, 2_d}, {8_t, 2_d}, {8_t, 2_d},
        {8_t, 3_d}, {8_t, 4_d}, {9_t, 0_d}, {9_t, 2_d}, {9_t, 3_d}, {9_t, 4_d}};
    using iterator_type = decltype(postings.begin());
    invert::Inverted_Index index;
    for (auto iter = postings.begin(); iter != postings.end(); ++iter) {
        index(tbb::blocked_range<iterator_type>(iter, std::next(iter)));
    }
    REQUIRE(
        index.documents
        == std::unordered_map<Term_Id, std::vector<Document_Id>>{{0_t, {0_d, 1_d, 4_d}},
                                                                 {1_t, {2_d, 4_d}},
                                                                 {2_t, {0_d, 1_d}},
                                                                 {3_t, {0_d, 1_d, 4_d}},
                                                                 {4_t, {1_d, 4_d}},
                                                                 {5_t, {1_d, 2_d, 3_d, 4_d}},
                                                                 {6_t, {1_d, 4_d}},
                                                                 {7_t, {1_d}},
                                                                 {8_t, {2_d, 3_d, 4_d}},
                                                                 {9_t, {0_d, 2_d, 3_d, 4_d}}});
    REQUIRE(
        index.frequencies
        == std::unordered_map<Term_Id, std::vector<Frequency>>{{0_t, {2_f, 1_f, 1_f}},
                                                               {1_t, {1_f, 1_f}},
                                                               {2_t, {1_f, 1_f}},
                                                               {3_t, {1_f, 1_f, 1_f}},
                                                               {4_t, {2_f, 1_f}},
                                                               {5_t, {2_f, 1_f, 1_f, 1_f}},
                                                               {6_t, {1_f, 4_f}},
                                                               {7_t, {1_f}},
                                                               {8_t, {3_f, 1_f, 1_f}},
                                                               {9_t, {1_f, 1_f, 1_f, 1_f}}});
}

TEST_CASE("Join Inverted_Index to another", "[invert][unit]")
{
    using index_type = invert::Inverted_Index;
    auto [lhs, rhs, expected_joined, message] =
        GENERATE(table<index_type, index_type, index_type, std::string>(
            {{index_type(
                  {{0_t, {0_d, 1_d, 2_d}}, {1_t, {0_d, 1_d}}, {2_t, {5_d}}},
                  {{0_t, {1_f, 1_f, 1_f}}, {1_t, {4_f, 1_f}}, {2_t, {1_f}}}),
              index_type(
                  {{3_t, {0_d, 1_d, 2_d}}, {4_t, {0_d, 1_d}}, {5_t, {5_d}}},
                  {{3_t, {1_f, 1_f, 1_f}}, {4_t, {4_f, 1_f}}, {5_t, {1_f}}}),
              index_type(
                  {{0_t, {0_d, 1_d, 2_d}},
                   {1_t, {0_d, 1_d}},
                   {2_t, {5_d}},
                   {3_t, {0_d, 1_d, 2_d}},
                   {4_t, {0_d, 1_d}},
                   {5_t, {5_d}}},
                  {{0_t, {1_f, 1_f, 1_f}},
                   {1_t, {4_f, 1_f}},
                   {2_t, {1_f}},
                   {3_t, {1_f, 1_f, 1_f}},
                   {4_t, {4_f, 1_f}},
                   {5_t, {1_f}}}),
              "disjoint terms"},
             {index_type(
                  {{0_t, {0_d, 1_d, 2_d}}, {1_t, {0_d, 1_d}}, {2_t, {5_d}}},
                  {{0_t, {1_f, 1_f, 1_f}}, {1_t, {4_f, 1_f}}, {2_t, {1_f}}}),
              index_type(
                  {{2_t, {6_d, 7_d, 8_d}}, {3_t, {0_d, 1_d}}, {4_t, {5_d}}},
                  {{2_t, {1_f, 1_f, 1_f}}, {3_t, {4_f, 1_f}}, {4_t, {1_f}}}),
              index_type(
                  {{0_t, {0_d, 1_d, 2_d}},
                   {1_t, {0_d, 1_d}},
                   {2_t, {5_d, 6_d, 7_d, 8_d}},
                   {3_t, {0_d, 1_d}},
                   {4_t, {5_d}}},
                  {{0_t, {1_f, 1_f, 1_f}},
                   {1_t, {4_f, 1_f}},
                   {2_t, {1_f, 1_f, 1_f, 1_f}},
                   {3_t, {4_f, 1_f}},
                   {4_t, {1_f}}}),
              "disjoint documents"},
             {index_type(
                  {{0_t, {0_d, 1_d, 2_d}}, {1_t, {0_d, 1_d}}, {2_t, {5_d}}},
                  {{0_t, {1_f, 1_f, 1_f}}, {1_t, {4_f, 1_f}}, {2_t, {1_f}}}),
              index_type(
                  {{2_t, {5_d, 7_d, 8_d}}, {3_t, {0_d, 1_d}}, {4_t, {5_d}}},
                  {{2_t, {1_f, 1_f, 1_f}}, {3_t, {4_f, 1_f}}, {4_t, {1_f}}}),
              index_type(
                  {{0_t, {0_d, 1_d, 2_d}},
                   {1_t, {0_d, 1_d}},
                   {2_t, {5_d, 7_d, 8_d}},
                   {3_t, {0_d, 1_d}},
                   {4_t, {5_d}}},
                  {{0_t, {1_f, 1_f, 1_f}},
                   {1_t, {4_f, 1_f}},
                   {2_t, {2_f, 1_f, 1_f}},
                   {3_t, {4_f, 1_f}},
                   {4_t, {1_f}}}),
              "overlapping term and document"},
             {index_type({{0_t, {0_d}}}, {{0_t, {1_f}}}),
              index_type({{0_t, {0_d}}}, {{0_t, {1_f}}}),
              index_type({{0_t, {0_d}}}, {{0_t, {2_f}}}),
              "single posting"}}));
    WHEN("Join left to right -- " << message)
    {
        lhs.join(rhs);
        REQUIRE(lhs.documents == expected_joined.documents);
        REQUIRE(lhs.frequencies == expected_joined.frequencies);
    }
    WHEN("Join right to left -- " << message)
    {
        rhs.join(lhs);
        REQUIRE(rhs.documents == expected_joined.documents);
        REQUIRE(rhs.frequencies == expected_joined.frequencies);
    }
}

TEST_CASE("Invert a range of documents from a collection", "[invert][unit]")
{
    using index_type = invert::Inverted_Index;
    std::vector<std::vector<Term_Id>> collection = {
        /* Doc 0 */ {2_t, 0_t, 3_t, 9_t, 0_t},
        /* Doc 1 */ {5_t, 0_t, 3_t, 4_t, 2_t, 6_t, 7_t, 4_t, 5_t},
        /* Doc 2 */ {5_t, 1_t, 8_t, 9_t, 8_t, 8_t},
        /* Doc 3 */ {8_t, 5_t, 9_t},
        /* Doc 4 */ {8_t, 6_t, 9_t, 6_t, 6_t, 5_t, 4_t, 3_t, 1_t, 0_t, 6_t}};

    std::vector<gsl::span<Term_Id const>> document_range;
    std::transform(
        collection.begin(), collection.end(), std::back_inserter(document_range), [](auto const& vec) {
            return gsl::span<Term_Id const>(vec);
        });
    size_t threads = 1;

    auto index = invert::invert_range(document_range, 0_d, threads);

    index_type expected(
        {{0_t, {0_d, 1_d, 4_d}},
         {1_t, {2_d, 4_d}},
         {2_t, {0_d, 1_d}},
         {3_t, {0_d, 1_d, 4_d}},
         {4_t, {1_d, 4_d}},
         {5_t, {1_d, 2_d, 3_d, 4_d}},
         {6_t, {1_d, 4_d}},
         {7_t, {1_d}},
         {8_t, {2_d, 3_d, 4_d}},
         {9_t, {0_d, 2_d, 3_d, 4_d}}},
        {{0_t, {2_f, 1_f, 1_f}},
         {1_t, {1_f, 1_f}},
         {2_t, {1_f, 1_f}},
         {3_t, {1_f, 1_f, 1_f}},
         {4_t, {2_f, 1_f}},
         {5_t, {2_f, 1_f, 1_f, 1_f}},
         {6_t, {1_f, 4_f}},
         {7_t, {1_f}},
         {8_t, {3_f, 1_f, 1_f}},
         {9_t, {1_f, 1_f, 1_f, 1_f}}},
        {5, 9, 6, 3, 11});
    REQUIRE(index.documents == expected.documents);
    REQUIRE(index.frequencies == expected.frequencies);
    REQUIRE(index.document_sizes == expected.document_sizes);
}

TEST_CASE("Invert collection", "[invert][unit]")
{
    GIVEN("A binary collection")
    {
        pisa::TemporaryDirectory tmpdir;
        uint32_t batch_size = GENERATE(1, 2, 3, 4, 5);
        uint32_t threads = GENERATE(1, 2, 3, 4, 5);
        invert::InvertParams params;
        params.batch_size = batch_size;
        params.num_threads = threads;
        bool with_lex = GENERATE(false, true);
        auto collection_filename = (tmpdir.path() / "fwd").string();
        {
            std::vector<uint32_t> collection_data{
                /* size */ 1,  /* count */ 5,
                /* size */ 5,  /* Doc 0 */ 2, 0, 3, 9, 0,
                /* size */ 9,  /* Doc 1 */ 5, 0, 3, 4, 2, 6, 7, 4, 5,
                /* size */ 6,  /* Doc 2 */ 5, 1, 8, 9, 8, 8,
                /* size */ 3,  /* Doc 3 */ 8, 5, 9,
                /* size */ 11, /* Doc 4 */ 8, 6, 9, 6, 6, 5, 4, 3, 1, 0, 6};
            std::ofstream os(collection_filename);
            os.write(
                reinterpret_cast<char*>(collection_data.data()),
                collection_data.size() * sizeof(uint32_t));
            if (with_lex) {
                std::vector<std::string> terms{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"};
                encode_payload_vector(terms.begin(), terms.end())
                    .to_file((tmpdir.path() / "fwd.termlex").string());
            }
        }
        WHEN("Run inverting with batch size " << batch_size << " and " << threads << " threads")
        {
            auto index_basename = (tmpdir.path() / "idx").string();
            if (not with_lex) {
                params.term_count = 10;
            }
            invert::invert_forward_index(collection_filename, index_basename, params);
            THEN("Index is stored in binary_freq_collection format")
            {
                std::vector<uint32_t> document_data{
                    /* size */ 1, /* count */ 5,
                    /* size */ 3, /* Term 0 */ 0, 1, 4,
                    /* size */ 2, /* Term 1 */ 2, 4,
                    /* size */ 2, /* Term 2 */ 0, 1,
                    /* size */ 3, /* Term 3 */ 0, 1, 4,
                    /* size */ 2, /* Term 4 */ 1, 4,
                    /* size */ 4, /* Term 5 */ 1, 2, 3, 4,
                    /* size */ 2, /* Term 6 */ 1, 4,
                    /* size */ 1, /* Term 7 */ 1,
                    /* size */ 3, /* Term 8 */ 2, 3, 4,
                    /* size */ 4, /* Term 9 */ 0, 2, 3, 4};
                std::vector<uint32_t> frequency_data{
                    /* size */ 3, /* Term 0 */ 2, 1, 1,
                    /* size */ 2, /* Term 1 */ 1, 1,
                    /* size */ 2, /* Term 2 */ 1, 1,
                    /* size */ 3, /* Term 3 */ 1, 1, 1,
                    /* size */ 2, /* Term 4 */ 2, 1,
                    /* size */ 4, /* Term 5 */ 2, 1, 1, 1,
                    /* size */ 2, /* Term 6 */ 1, 4,
                    /* size */ 1, /* Term 7 */ 1,
                    /* size */ 3, /* Term 8 */ 3, 1, 1,
                    /* size */ 4, /* Term 9 */ 1, 1, 1, 1};
                std::vector<uint32_t> size_data{/* size */ 5, /* sizes */ 5, 9, 6, 3, 11};
                mio::mmap_source mm;
                std::error_code error;
                mm.map((index_basename + ".docs").c_str(), error);
                std::vector<uint32_t> d(
                    reinterpret_cast<uint32_t const*>(mm.data()),
                    reinterpret_cast<uint32_t const*>(mm.data()) + mm.size() / sizeof(uint32_t));
                mio::mmap_source mmf;
                mmf.map((index_basename + ".freqs").c_str(), error);
                std::vector<uint32_t> f(
                    reinterpret_cast<uint32_t const*>(mmf.data()),
                    reinterpret_cast<uint32_t const*>(mmf.data()) + mmf.size() / sizeof(uint32_t));
                mio::mmap_source mms;
                mms.map((index_basename + ".sizes").c_str(), error);
                std::vector<uint32_t> s(
                    reinterpret_cast<uint32_t const*>(mms.data()),
                    reinterpret_cast<uint32_t const*>(mms.data()) + mms.size() / sizeof(uint32_t));
                REQUIRE(d == document_data);
                REQUIRE(f == frequency_data);
                REQUIRE(s == size_data);
                auto batch_files = pisa::ls(tmpdir.path().string(), [](auto const& filename) {
                    return filename.find("batch") != std::string::npos;
                });
                REQUIRE(batch_files.empty());
            }
        }
    }
}
