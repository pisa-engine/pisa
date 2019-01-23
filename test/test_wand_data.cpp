#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <functional>

#include "test_common.hpp"

#include "ds2i_config.hpp"
#include "index_types.hpp"
#include "query/queries.hpp"
#include "wand_data_range.hpp"

using namespace pisa;

TEST_CASE("wand_data_range") {
    typedef wand_data_range<64, 1024, bm25> WandTypeRange;
    typedef wand_data<bm25, WandTypeRange> WandType;
    using Scorer = bm25;

    binary_freq_collection collection(DS2I_SOURCE_DIR "/test/test_data/test_collection");
    binary_collection      document_sizes(DS2I_SOURCE_DIR "/test/test_data/test_collection.sizes");
    WandType          wdata_range(document_sizes.begin()->begin(),
                              collection.num_docs(),
                              collection);
    SECTION( "Precomputed block-max scores" ) {
        size_t i = 0;
        for (auto const &seq: collection) {
            if(seq.docs.size() >= 1024) {
                auto max = wdata_range.max_term_weight(i);
                auto w   = wdata_range.getenum(i);
                for (auto j = 0; j < seq.docs.size(); ++j) {
                    auto docid = *(seq.docs.begin() + j);
                    auto freq = *(seq.freqs.begin() + j);
                    float score = Scorer::doc_term_weight(freq, wdata_range.norm_len(docid));
                    w.next_geq(docid);
                    CHECKED_ELSE(w.score() >= score) {
                        FAIL("Term: " << i << " docid: " << docid << ", block docid: " << w.docid());
                    }
                    REQUIRE(w.score() <= max);
                }
            }
            i+=1;
        }
    }
    typedef opt_index index_type;
    index_type               index;
    global_parameters        params;
    index_type::builder builder(collection.num_docs(), params);
    for (auto const &plist : collection) {
        uint64_t freqs_sum =
            std::accumulate(plist.freqs.begin(), plist.freqs.end(), uint64_t(0));
        builder.add_posting_list(
            plist.docs.size(), plist.docs.begin(), plist.freqs.begin(), freqs_sum);
    }
    builder.build(index);

    SECTION( "Compute at run time" ) {
        size_t i =0;
        for (auto const &seq: collection) {
            auto list = index[i];
            if(seq.docs.size() < 1024) {
                auto max = wdata_range.max_term_weight(i);
                auto &w   = wdata_range.get_block_wand();
                const mapper::mappable_vector<float> bm = w.compute(list, [&](uint64_t d) { return wdata_range.norm_len(d); });
                WandTypeRange::enumerator we(0, bm);
                for (auto j = 0; j < seq.docs.size(); ++j) {
                    auto docid = *(seq.docs.begin() + j);
                    auto freq = *(seq.freqs.begin() + j);
                    float score = Scorer::doc_term_weight(freq, wdata_range.norm_len(docid));
                    we.next_geq(docid);
                    CHECKED_ELSE(we.score() >= score) {
                        FAIL("Term: " << i << " docid: " << docid <<", pos: " << j << ", block docid: " << we.docid());
                    }
                    REQUIRE(we.score() <= max);
                }
            }
            i+=1;
        }
    }

}
