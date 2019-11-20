#pragma once

#include "../temporary_directory.hpp"
#include "pisa_config.hpp"
#include "query/queries.hpp"
#include "v1/index.hpp"
#include "v1/index_builder.hpp"
#include "v1/query.hpp"
#include "v1/score_index.hpp"

namespace v1 = pisa::v1;

[[nodiscard]] inline auto test_queries() -> std::vector<pisa::Query>
{
    std::vector<pisa::Query> queries;
    std::ifstream qfile(PISA_SOURCE_DIR "/test/test_data/queries");
    auto push_query = [&](std::string const& query_line) {
        queries.push_back(pisa::parse_query_ids(query_line));
    };
    pisa::io::for_each_line(qfile, push_query);
    return queries;
}

template <typename DocumentCursor, typename FrequencyCursor, typename ScoreCursor>
struct IndexFixture {
    using DocumentWriter = typename v1::CursorTraits<DocumentCursor>::Writer;
    using FrequencyWriter = typename v1::CursorTraits<FrequencyCursor>::Writer;
    using ScoreWriter = typename v1::CursorTraits<ScoreCursor>::Writer;

    using DocumentReader = typename v1::CursorTraits<DocumentCursor>::Reader;
    using FrequencyReader = typename v1::CursorTraits<FrequencyCursor>::Reader;
    using ScoreReader = typename v1::CursorTraits<ScoreCursor>::Reader;

    IndexFixture() : m_tmpdir(std::make_unique<Temporary_Directory>())
    {
        auto index_basename = (tmpdir().path() / "inv").string();
        v1::compress_binary_collection(PISA_SOURCE_DIR "/test/test_data/test_collection",
                                       PISA_SOURCE_DIR "/test/test_data/test_collection.fwd",
                                       index_basename,
                                       2,
                                       v1::make_writer<DocumentWriter>(),
                                       v1::make_writer<FrequencyWriter>());
        auto errors = v1::verify_compressed_index(PISA_SOURCE_DIR "/test/test_data/test_collection",
                                                  index_basename);
        REQUIRE(errors.empty());
        v1::score_index(fmt::format("{}.yml", index_basename), 1);
    }

    [[nodiscard]] auto const& tmpdir() const { return *m_tmpdir; }
    [[nodiscard]] auto document_reader() const { return m_document_reader; }
    [[nodiscard]] auto frequency_reader() const { return m_frequency_reader; }
    [[nodiscard]] auto score_reader() const { return m_score_reader; }
    [[nodiscard]] auto meta() const
    {
        auto index_basename = (tmpdir().path() / "inv").string();
        return v1::IndexMetadata::from_file(fmt::format("{}.yml", index_basename));
    }

   private:
    std::unique_ptr<Temporary_Directory const> m_tmpdir;
    DocumentReader m_document_reader{};
    FrequencyReader m_frequency_reader{};
    ScoreReader m_score_reader{};
};
