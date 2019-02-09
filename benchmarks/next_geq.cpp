#include <random>

#include <celero/Celero.h>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/zip.hpp>
#include <spdlog/sinks/null_sink.h>

#include "accumulator/lazy_accumulator.hpp"
#include "ds2i_config.hpp"
#include "index_types.hpp"
#include "query/queries.hpp"
#include "query/scored_range.hpp"

CELERO_MAIN

using namespace pisa;

struct Vector_Cursor {
    [[nodiscard]] auto docid() const noexcept -> uint32_t { return *documents_; }
    [[nodiscard]] auto score() const noexcept -> float { return *scores_; }
    void next_geq(uint32_t doc)
    {
        while (*documents_ < doc) {
            ++documents_;
            ++scores_;
        }
    }

    std::vector<uint32_t>::const_iterator documents_;
    std::vector<float>::const_iterator scores_;
};

struct Vector_Range {
    template <typename Scored_Range>
    Vector_Range(Scored_Range const &range)
    {
        auto size = range.size();
        documents_.reserve(size);
        scores_.reserve(size);
        auto cursor = range.cursor();
        for (; cursor.docid() < pisa::cursor::document_bound; cursor.next()) {
            documents_.push_back(cursor.docid());
            scores_.push_back(cursor.score());
        }
        documents_.push_back(pisa::cursor::document_bound);
    }
    [[nodiscard]] auto cursor() const noexcept
    {
        return Vector_Cursor{documents_.begin(), scores_.begin()};
    }

    std::vector<uint32_t> documents_{};
    std::vector<float> scores_{};
};

template <typename Index = single_index, typename Wand = wand_data<bm25, wand_data_raw<bm25>>>
class Index_Fixture : public celero::TestFixture {
   public:
    using index_type = Index;
    using wand_type = Wand;
    using score_function_type = Score_Function<bm25, wand_type>;
    using freq_range_type = typename Index::Posting_Range;
    using freq_cursor_type = decltype(std::declval<freq_range_type>().cursor());
    using scored_range_type = Scored_Range<freq_range_type, score_function_type>;
    using scored_cursor_type = decltype(std::declval<scored_range_type>().cursor());

    Index_Fixture()
        : collection(DS2I_SOURCE_DIR "/test/test_data/test_collection"),
          document_sizes(DS2I_SOURCE_DIR "/test/test_data/test_collection.sizes"),
          wdata(document_sizes.begin()->begin(), collection.num_docs(), collection)
    {
        auto sink = std::make_shared<spdlog::sinks::null_sink_st>();
        spdlog::set_default_logger(std::make_shared<spdlog::logger>("null", sink));
        typename index_type::builder builder(collection.num_docs(), params);
        for (auto const &plist : collection) {
            uint64_t freqs_sum =
                std::accumulate(plist.freqs.begin(), plist.freqs.end(), uint64_t(0));
            builder.add_posting_list(
                plist.docs.size(), plist.docs.begin(), plist.freqs.begin(), freqs_sum);
        }
        builder.build(index);

        term_id_vec q;
        std::ifstream qfile(DS2I_SOURCE_DIR "/test/test_data/queries");
        while (read_query(q, qfile)) {
            queries.push_back(q);
        }

        std::string t;
        std::ifstream tin(DS2I_SOURCE_DIR "/test/test_data/top5_thresholds");
        while (std::getline(tin, t)) {
            thresholds.push_back(std::stof(t));
        }
    }

    virtual std::vector<celero::TestFixture::ExperimentValue> getExperimentValues() const override
    {
        return std::vector<celero::TestFixture::ExperimentValue>{
            uint64_t{100}, uint64_t{500}, uint64_t{1000}, uint64_t{5000}};
    }

    void setUp(const celero::TestFixture::ExperimentValue &stride) override
    {
        uint32_t first_term = 0u;
        uint32_t last_term = 1000u;
        for (auto term_id : ranges::view::iota(first_term, last_term)) {
            freq_ranges.push_back(index.posting_range(term_id));
            freq_cursors.push_back(freq_ranges.back().cursor());
            auto q_weight =
                bm25::query_term_weight(1, freq_cursors.back().size(), index.num_docs());
            scorers.push_back({q_weight, std::cref(wdata)});
            scored_ranges.emplace_back(index.posting_range(term_id), scorers.back());
            scored_cursors.push_back(scored_ranges.back().cursor());
            vector_ranges.emplace_back(
                scored_range_type(index.posting_range(term_id), scorers.back()));
            vector_cursors.push_back(vector_ranges.back().cursor());
            enumerators.push_back(index[term_id]);
        }
        this->stride = stride.Value;
    }

    void tearDown() override
    {
        freq_ranges.clear();
        freq_cursors.clear();
        scorers.clear();
        scored_ranges.clear();
        scored_cursors.clear();
        vector_ranges.clear();
        vector_cursors.clear();
    }

    global_parameters params;
    binary_freq_collection collection;
    binary_collection document_sizes;
    index_type index;
    std::vector<term_id_vec> queries;
    std::vector<float> thresholds;
    wand_type wdata;

    std::vector<typename Index::document_enumerator> enumerators;
    std::vector<Vector_Range> vector_ranges;
    std::vector<Vector_Cursor> vector_cursors;
    std::vector<freq_range_type> freq_ranges;
    std::vector<freq_cursor_type> freq_cursors;
    std::vector<score_function_type> scorers;
    std::vector<scored_range_type> scored_ranges;
    std::vector<scored_cursor_type> scored_cursors;
    uint64_t stride = 0;
};

BASELINE_F(Next_GEQ, Vectors, Index_Fixture<pisa::block_simdbp_index>, 50, 1)
{
    for (auto &&cursor : vector_cursors) {
        uint32_t doc = 0;
        for (; cursor.docid() < pisa::cursor::document_bound; cursor.next_geq(doc)) {
            celero::DoNotOptimizeAway(cursor.score());
            doc += stride;
        }
    }
}

BENCHMARK_F(Next_GEQ, enumerator, Index_Fixture<pisa::block_simdbp_index>, 50, 1)
{
    for (auto &&[enumerator, scorer] : ranges::view::zip(enumerators, scorers)) {
        uint32_t doc = 0;
        for (; enumerator.docid() < index.num_docs(); enumerator.next_geq(doc)) {
            celero::DoNotOptimizeAway(scorer(enumerator.docid(), enumerator.freq()));
            doc += stride;
        }
    }
}

BENCHMARK_F(Next_GEQ, Freq_Cursor, Index_Fixture<pisa::block_simdbp_index>, 50, 1)
{
    for (auto &&[cursor, scorer] : ranges::view::zip(freq_cursors, scorers)) {
        uint32_t doc = 0;
        for (; cursor.docid() < pisa::cursor::document_bound; cursor.next_geq(doc)) {
            celero::DoNotOptimizeAway(scorer(cursor.docid(), cursor.freq()));
            doc += stride;
        }
    }
}

BENCHMARK_F(Next_GEQ, Scored_Cursor, Index_Fixture<pisa::block_simdbp_index>, 50, 1)
{
    for (auto &&cursor : scored_cursors) {
        uint32_t doc = 0;
        for (; cursor.docid() < pisa::cursor::document_bound; cursor.next_geq(doc)) {
            celero::DoNotOptimizeAway(cursor.score());
            doc += stride;
        }
    }
}
