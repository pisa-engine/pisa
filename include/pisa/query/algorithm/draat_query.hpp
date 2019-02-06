#pragma once

#include "query/queries.hpp"
#include "scorer/bm25.hpp"
#include "topk_queue.hpp"
#include "util/intrinsics.hpp"

namespace pisa {

template <typename Range_Algorithm>
class draat_query {
   public:
    draat_query(Range_Algorithm &&algorithm, uint32_t range_size, uint32_t document_count, int k)
        : topk_(k),
          range_algorithm_(algorithm),
          range_size_(range_size),
          document_count_(document_count)
    {}

    template <typename Scored_Range>
    auto operator()(gsl::span<Scored_Range> posting_ranges) -> int64_t
    {
        topk_.clear();
        if (posting_ranges.empty()) {
            return 0;
        }
        for (uint32_t first_document = 0u; first_document < document_count_;
             first_document += range_size_)
        {
            uint32_t last_document = std::min(first_document + range_size_, document_count_);
            std::vector<std::decay_t<Scored_Range>> subranges;
            std::transform(posting_ranges.begin(),
                           posting_ranges.end(),
                           std::back_inserter(subranges),
                           [&](auto const &range) { return range(first_document, last_document); });
            range_algorithm_(gsl::make_span(subranges));
            for (auto [score, docid] : range_algorithm_.topk()) {
                topk_.insert(score, docid);
            }
        }
        topk_.finalize();
        return topk_.topk().size();
    }

    std::vector<std::pair<float, uint64_t>> const &topk() const { return topk_.topk(); }

   private:
    topk_queue topk_;
    Range_Algorithm range_algorithm_;
    uint32_t range_size_;
    uint32_t document_count_;
};

}; // namespace pisa
