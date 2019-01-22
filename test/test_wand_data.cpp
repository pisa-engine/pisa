#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "test_common.hpp"

#include "ds2i_config.hpp"
#include "index_types.hpp"
#include "query/queries.hpp"
#include "wand_data_range.hpp"

using namespace pisa;

TEST_CASE("wand_data_range") {
    typedef wand_data<bm25, wand_data_range<1024, bm25>> WandTypeRange;
    using Scorer = bm25;

    binary_freq_collection collection(DS2I_SOURCE_DIR "/test/test_data/test_collection");
    binary_collection      document_sizes(DS2I_SOURCE_DIR "/test/test_data/test_collection.sizes");
    WandTypeRange          wdata_range(document_sizes.begin()->begin(),
                              collection.num_docs(),
                              collection);

    size_t i = 0;
    for (auto const &seq: collection) {
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

        i+=1;
    }

}
