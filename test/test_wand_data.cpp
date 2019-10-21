#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <functional>

#include <range/v3/view/enumerate.hpp>
#include <tbb/task_scheduler_init.h>

#include "test_common.hpp"

#include "index_types.hpp"
#include "pisa_config.hpp"
#include "query/queries.hpp"
#include "scorer/score_function.hpp"
#include "wand_data_range.hpp"

using namespace pisa;

TEST_CASE("wand_data_range")
{
    tbb::task_scheduler_init init;
    using WandTypeRange = wand_data_range<64, 1024, bm25>;
    using WandType = wand_data<WandTypeRange>;
    using Scorer = bm25;

    binary_freq_collection const collection(PISA_SOURCE_DIR "/test/test_data/test_collection");
    binary_collection document_sizes(PISA_SOURCE_DIR "/test/test_data/test_collection.sizes");
    WandType wdata_range(document_sizes.begin()->begin(),
                         collection.num_docs(),
                         collection,
                         BlockSize(FixedBlock()));

    SECTION("Precomputed block-max scores")
    {
        size_t term_id = 0;
        for (auto const &seq : collection) {
            if (seq.docs.size() >= 1024) {
                auto max = wdata_range.max_term_weight(term_id);
                auto w = wdata_range.getenum(term_id);
                for (auto &&[docid, freq] : ranges::view::zip(seq.docs, seq.freqs)) {
                    float score = Scorer::doc_term_weight(freq, wdata_range.norm_len(docid));
                    w.next_geq(docid);
                    CHECKED_ELSE(w.score() >= score)
                    {
                        FAIL("Term: " << term_id << " docid: " << docid
                                      << ", block docid: " << w.docid());
                    }
                    REQUIRE(w.score() <= max);
                }
            }
            term_id += 1;
        }
    }
    using index_type = pefopt_index;
    index_type index;
    global_parameters params;
    index_type::builder builder(collection.num_docs(), params);
    for (auto const &plist : collection) {
        uint64_t freqs_sum = std::accumulate(plist.freqs.begin(), plist.freqs.end(), uint64_t(0));
        builder.add_posting_list(
            plist.docs.size(), plist.docs.begin(), plist.freqs.begin(), freqs_sum);
    }
    builder.build(index);

    SECTION("Compute at run time")
    {
        size_t term_id = 0;
        for (auto const &seq : collection) {
            auto list = index[term_id];
            if (seq.docs.size() < 1024) {
                auto max = wdata_range.max_term_weight(term_id);
                auto &w = wdata_range.get_block_wand();
                Score_Function<Scorer, WandType> score_func{1.f, wdata_range};
                const mapper::mappable_vector<float> bm =
                    w.compute_block_max_scores(list, score_func);
                WandTypeRange::enumerator we(0, bm);
                for (auto &&[pos, docid, freq] :
                     ranges::view::zip(ranges::view::iota(0), seq.docs, seq.freqs)) {
                    float score = Scorer::doc_term_weight(freq, wdata_range.norm_len(docid));
                    we.next_geq(docid);
                    CHECKED_ELSE(we.score() >= score)
                    {
                        FAIL("Term: " << term_id << " docid: " << docid << ", pos: " << pos
                                      << ", block docid: " << we.docid());
                    }
                    REQUIRE(we.score() <= max);
                }
            }
            term_id += 1;
        }
    }

    SECTION("Live block computation")
    {
        size_t i = 0;
        std::vector<WandTypeRange::enumerator> enums;
        for (auto const &seq : collection) {
            if (seq.docs.size() >= 1024) {
                enums.push_back(wdata_range.getenum(i));
            }
        }

        std::pair<uint32_t, uint32_t> doc_range(0, collection.num_docs());
        auto live_blocks = WandTypeRange::compute_live_blocks(enums, 0, doc_range);

        REQUIRE(live_blocks.size() == ceil_div(collection.num_docs(), 64));

        for (int i = 0; i < live_blocks.size(); ++i) {
            if (live_blocks[i] == 0) {
                for (auto &&e : enums) {
                    e.next_geq(i * 64);
                    REQUIRE(e.score() == 0);
                }
            }
        }
    }
}
