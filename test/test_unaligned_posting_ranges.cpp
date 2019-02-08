#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <range/v3/view/enumerate.hpp>

#include "algorithm/numeric.hpp"
#include "ds2i_config.hpp"
#include "index_types.hpp"
#include "query/queries.hpp"
#include "query/scored_range.hpp"

using namespace pisa;

template <typename Index>
struct Index_Data {
    using index_type = Index;
    using wand_type = wand_data<bm25, wand_data_raw<bm25>>;

    Index_Data()
        : collection(DS2I_SOURCE_DIR "/test/test_data/test_collection"),
          document_sizes(DS2I_SOURCE_DIR "/test/test_data/test_collection.sizes"),
          wdata(document_sizes.begin()->begin(), collection.num_docs(), collection)
    {
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
    }

    global_parameters params;
    binary_freq_collection collection;
    binary_collection document_sizes;
    index_type index;
    std::vector<term_id_vec> queries;
    wand_type wdata;
};

struct Bruteforce_Range_Query {
    Bruteforce_Range_Query(int doc_count, int k, std::vector<std::pair<uint32_t, uint32_t>> spans)
        : top_k_(k), acc_(doc_count), spans_(std::move(spans))
    {}

    template <typename Scored_Range>
    auto operator()(gsl::span<Scored_Range> posting_ranges) -> int64_t
    {
        top_k_.clear();
        if (posting_ranges.empty()) {
            return 0;
        }
        acc_.init();
        int idx = 0;
        for (auto const &range : posting_ranges) {
            uint32_t first = idx < spans_.size() ? spans_[idx].first : 0;
            uint32_t last = idx < spans_.size() ? spans_[idx].second : pisa::cursor::document_bound;
            auto cursor = range.cursor();
            for (; cursor.docid() < pisa::cursor::document_bound; cursor.next()) {
                if (between(first, last)(cursor.docid())) {
                    acc_.accumulate(cursor.docid(), cursor.score());
                }
            }
            ++idx;
        }
        acc_.aggregate(top_k_);
        top_k_.finalize();
        return topk().size();
    }

    std::vector<std::pair<float, uint64_t>> const &topk() const { return top_k_.topk(); }

   private:
    pisa::topk_queue top_k_;
    pisa::Simple_Accumulator acc_;
    std::vector<std::pair<uint32_t, uint32_t>> spans_;
};

template <typename Baseline, typename F, typename Query_Fn, typename P>
void test(Baseline baseline, F &&full_postings, Query_Fn query_fn, P &&postings)
{
    baseline(gsl::make_span(full_postings));
    query_fn(gsl::make_span(postings));
    REQUIRE(baseline.topk().size() == query_fn.topk().size());
    for (size_t i = 0; i < baseline.topk().size(); ++i) {
        REQUIRE(baseline.topk()[i].first == Approx(query_fn.topk()[i].first).epsilon(0.1));
    }
}

TEST_CASE("Test querying with unaligned posting ranges", "[query][ranges]")
{
    auto spans = std::vector<std::pair<uint32_t, uint32_t>>{
        {14, 1001}, {14, 1001}, {50, 90}, {50, 10000}, {5000, 7000}};
    Index_Data<block_simdbp_index> data;
    auto get_postings = [&data, &spans](auto const &q) {
        auto ranges = max_scored_ranges(data.index, data.wdata, q);
        std::vector<std::decay_t<decltype(ranges[0])>> subranges;
        for (auto &&[idx, range] : ranges::view::enumerate(ranges)) {
            if (idx < spans.size()) {
                subranges.push_back(range(spans[idx].first, spans[idx].second));
            }
            else {
                subranges.push_back(range(0, pisa::cursor::document_bound));
            }
        }
        return subranges;
    };
    Bruteforce_Range_Query baseline(data.index.num_docs(), 10, spans);

    SECTION("ranked_or_taat")
    {
        for (auto const &q : data.queries) {
            test(baseline,
                 max_scored_ranges(data.index, data.wdata, q),
                 pisa::make_ranked_or_taat_query<pisa::Simple_Accumulator>(
                     data.index, data.wdata, 10),
                 get_postings(q));
        }
    }

    SECTION("ranked_or")
    {
        for (auto const &q : data.queries) {
            test(baseline,
                 max_scored_ranges(data.index, data.wdata, q),
                 pisa::ranked_or_query(data.index, data.wdata, 10),
                 get_postings(q));
        }
    }

    SECTION("wand")
    {
        for (auto const &q : data.queries) {
            test(baseline,
                 max_scored_ranges(data.index, data.wdata, q),
                 pisa::wand_query(data.index, data.wdata, 10),
                 get_postings(q));
        }
    }

    SECTION("maxscore")
    {
        for (auto const &q : data.queries) {
            test(baseline,
                 max_scored_ranges(data.index, data.wdata, q),
                 pisa::maxscore_query(data.index, data.wdata, 10),
                 get_postings(q));
        }
    }
}
