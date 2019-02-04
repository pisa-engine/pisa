#pragma once

#include <gsl/span>

#include "query/queries.hpp"

namespace pisa {

template <typename Frequency_Cursor, typename Term_Scorer>
class Scored_Cursor {
   public:
    explicit Scored_Cursor(Frequency_Cursor &&freq_cursor, Term_Scorer scorer)
        : freq_cursor_(freq_cursor), scorer_(std::move(scorer))
    {}
    Scored_Cursor(Scored_Cursor const &) = default;
    Scored_Cursor(Scored_Cursor &&) noexcept = default;
    Scored_Cursor &operator=(Scored_Cursor const &) = default;
    Scored_Cursor& operator=(Scored_Cursor &&) noexcept = default;
    ~Scored_Cursor() = default;

    void reset() { freq_cursor_.reset(); }
    void DS2I_FLATTEN_FUNC next() { freq_cursor_.next(); }
    void DS2I_FLATTEN_FUNC next_geq(uint64_t lower_bound) { freq_cursor_.next_geq(lower_bound); }
    void DS2I_FLATTEN_FUNC move(uint64_t position) { freq_cursor_.move(position); }
    [[nodiscard]] auto docid() const { return freq_cursor_.docid(); }
    [[nodiscard]] auto score() { return scorer_(freq_cursor_.docid(), freq_cursor_.freq()); }
    [[nodiscard]] auto position() const { return freq_cursor_.position(); }

   private:
    Frequency_Cursor freq_cursor_;
    Term_Scorer scorer_;
};

template <typename Frequency_Range, typename Term_Scorer>
class Scored_Range {
   public:
    explicit Scored_Range(Frequency_Range &&freq_range, Term_Scorer scorer)
        : freq_range_(std::move(freq_range)), scorer_(std::move(scorer))
    {}
    Scored_Range(Scored_Range const &) = delete;
    Scored_Range(Scored_Range &&) noexcept = default;
    Scored_Range &operator=(Scored_Range const &) = delete;
    Scored_Range& operator=(Scored_Range &&) noexcept = default;
    ~Scored_Range() = default;

    [[nodiscard]] auto size() const -> int64_t { return freq_range_.size(); }
    [[nodiscard]] auto first_document() const { return freq_range_.first_document(); }
    [[nodiscard]] auto last_document() const { return freq_range_.last_document(); }
    [[nodiscard]] auto cursor() const { return Scored_Cursor{freq_range_.cursor(), scorer_}; }

   private:
    Frequency_Range freq_range_;
    Term_Scorer scorer_;
};

template <typename Index, typename WandType>
[[nodiscard]] auto scored_ranges(Index const &index, WandType const &wdata, term_id_vec terms)
{
    using scorer_type = bm25;
    using range_type = typename Index::Posting_Range;
    using score_function_type = Score_Function<scorer_type, WandType>;
    using scored_range_type = Scored_Range<range_type, score_function_type>;

    std::vector<scored_range_type> ranges;
    for (auto term : query_freqs(terms)) {
        auto freq_range = index.posting_range(term.first);
        auto q_weight =
            scorer_type::query_term_weight(term.second, freq_range.size(), index.num_docs());
        ranges.emplace_back(std::move(freq_range), score_function_type{q_weight, std::cref(wdata)});
    }
    return ranges;
}

}; // namespace pisa
