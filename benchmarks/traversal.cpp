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
        while (read_query(q, qfile))
            queries.push_back(q);

        std::string t;
        std::ifstream tin(DS2I_SOURCE_DIR "/test/test_data/top5_thresholds");
        while (std::getline(tin, t)) {
            thresholds.push_back(std::stof(t));
        }
    }

    void setUp(const celero::TestFixture::ExperimentValue &x) override
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
        }
    }

    void tearDown() override
    {
        freq_ranges.clear();
        freq_cursors.clear();
        scorers.clear();
        scored_ranges.clear();
        scored_cursors.clear();
    }

    global_parameters params;
    binary_freq_collection collection;
    binary_collection document_sizes;
    index_type index;
    std::vector<term_id_vec> queries;
    std::vector<float> thresholds;
    wand_type wdata;

    std::vector<freq_range_type> freq_ranges;
    std::vector<freq_cursor_type> freq_cursors;
    std::vector<score_function_type> scorers;
    std::vector<scored_range_type> scored_ranges;
    std::vector<scored_cursor_type> scored_cursors;
};

BASELINE_F(Traversal, Freq_Cursor, Index_Fixture<>, 100, 1)
{
    for (auto &&[range, cursor, scorer] : ranges::view::zip(freq_ranges, freq_cursors, scorers)) {
        for (; cursor.docid() < range.last_document(); cursor.next()) {
            celero::DoNotOptimizeAway(scorer(cursor.docid(), cursor.freq()));
        }
    }
}

BENCHMARK_F(Traversal, Scored_Cursor, Index_Fixture<>, 100, 1)
{
    for (auto &&[range, cursor] : ranges::view::zip(scored_ranges, scored_cursors)) {
        for (; cursor.docid() < range.last_document(); cursor.next()) {
            celero::DoNotOptimizeAway(cursor.score());
        }
    }
}
