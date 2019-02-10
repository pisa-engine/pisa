#pragma once

#include <memory>

#include <gsl/span>
#include <range/v3/view/all.hpp>
#include <range/v3/view/transform.hpp>

#include "cursor.hpp"
#include "query/queries.hpp"

namespace pisa {

template <typename T>
[[nodiscard]] constexpr auto noexcept_move_assignment() -> bool
{
    return noexcept(std::declval<T &>() = std::declval<T &&>());
}

template <typename Frequency_Cursor, typename Term_Scorer>
class Scored_Cursor {
   public:
    Scored_Cursor(Frequency_Cursor &&freq_cursor, Term_Scorer scorer)
        : freq_cursor_(freq_cursor), scorer_(std::move(scorer))
    {}
    Scored_Cursor(Scored_Cursor const &) = default;
    Scored_Cursor(Scored_Cursor &&) noexcept(
        noexcept(std::move(std::declval<Frequency_Cursor>())) and
        noexcept(std::move(std::declval<Term_Scorer>()))) = default;
    Scored_Cursor &operator=(Scored_Cursor const &) = default;
    Scored_Cursor &operator=(Scored_Cursor &&other) noexcept(
        noexcept_move_assignment<Frequency_Cursor>() and
        noexcept_move_assignment<Term_Scorer>()) = default;
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
    using document_type = uint32_t;
    using cursor_type = Scored_Cursor<typename Frequency_Range::cursor_type, Term_Scorer>;

    explicit Scored_Range(Frequency_Range &&freq_range, Term_Scorer scorer)
        : freq_range_(std::move(freq_range)), scorer_(std::move(scorer))
    {}
    Scored_Range(Scored_Range const &) = delete;
    Scored_Range(Scored_Range &&) noexcept(noexcept(std::move(std::declval<Frequency_Range>())) and
                                           noexcept(std::move(std::declval<Term_Scorer>()))) =
        default;
    Scored_Range &operator=(Scored_Range const &) = delete;
    Scored_Range &operator=(Scored_Range &&) = default;
    ~Scored_Range() = default;

    [[nodiscard]] auto size() const -> int64_t { return freq_range_.size(); }
    [[nodiscard]] auto first_document() const { return freq_range_.first_document(); }
    [[nodiscard]] auto last_document() const { return freq_range_.last_document(); }
    [[nodiscard]] auto cursor() const -> cursor_type
    {
        return Scored_Cursor{freq_range_.cursor(), scorer_};
    }
    [[nodiscard]] auto operator()(document_type low, document_type hi) const
    {
        return Scored_Range(freq_range_(low, hi), scorer_);
    }

   private:
    Frequency_Range freq_range_;
    Term_Scorer scorer_;
};

template <typename Frequency_Cursor, typename Term_Scorer>
class Max_Scored_Cursor {
   public:
    using scored_cursor_type = Scored_Cursor<Frequency_Cursor, Term_Scorer>;

    explicit Max_Scored_Cursor(scored_cursor_type &&scored_cursor, float max_score)
        : scored_cursor_(std::forward<scored_cursor_type>(scored_cursor)), max_score_(max_score)
    {}
    Max_Scored_Cursor(Max_Scored_Cursor const &) = default;
    Max_Scored_Cursor(Max_Scored_Cursor &&) noexcept(
        noexcept(std::move(std::declval<Frequency_Cursor>())) and
        noexcept(std::move(std::declval<Term_Scorer>()))) = default;
    Max_Scored_Cursor &operator=(Max_Scored_Cursor const &) = default;
    Max_Scored_Cursor &operator=(Max_Scored_Cursor &&) noexcept(
        noexcept_move_assignment<scored_cursor_type>()) = default;
    ~Max_Scored_Cursor() = default;

    void reset() { scored_cursor_.reset(); }
    void DS2I_FLATTEN_FUNC next() { scored_cursor_.next(); }
    void DS2I_FLATTEN_FUNC next_geq(uint64_t lower_bound) { scored_cursor_.next_geq(lower_bound); }
    void DS2I_FLATTEN_FUNC move(uint64_t position) { scored_cursor_.move(position); }
    [[nodiscard]] auto docid() const { return scored_cursor_.docid(); }
    [[nodiscard]] auto score() { return scored_cursor_.score(); }
    [[nodiscard]] auto position() const { return scored_cursor_.position(); }
    [[nodiscard]] auto max_score() const noexcept -> float { return max_score_; }

   private:
    scored_cursor_type scored_cursor_;
    float max_score_;
};

template <typename Frequency_Range, typename Term_Scorer>
class Max_Scored_Range {
   public:
    using scored_range_type = Scored_Range<Frequency_Range, Term_Scorer>;
    using cursor_type = Max_Scored_Cursor<typename Frequency_Range::cursor_type, Term_Scorer>;
    using document_type = uint32_t;

    explicit Max_Scored_Range(scored_range_type &&scored_range, float max_score)
        : scored_range_(std::forward<scored_range_type>(scored_range)), max_score_(max_score)
    {}
    Max_Scored_Range(Max_Scored_Range const &) = delete;
    Max_Scored_Range(Max_Scored_Range &&) noexcept(
        noexcept(std::move(std::declval<Frequency_Range>())) and
        noexcept(std::move(std::declval<Term_Scorer>()))) = default;
    Max_Scored_Range &operator=(Max_Scored_Range const &) = delete;
    Max_Scored_Range &operator=(Max_Scored_Range &&) = default;
    ~Max_Scored_Range() = default;

    [[nodiscard]] auto size() const -> int64_t { return scored_range_.size(); }
    [[nodiscard]] auto first_document() const { return scored_range_.first_document(); }
    [[nodiscard]] auto last_document() const { return scored_range_.last_document(); }
    [[nodiscard]] auto cursor() const -> cursor_type
    {
        return Max_Scored_Cursor{scored_range_.cursor(), max_score_};
    }
    [[nodiscard]] auto operator()(document_type low, document_type hi) const
    {
        return Max_Scored_Range(scored_range_(low, hi), max_score_);
    }
    [[nodiscard]] auto max_score() const noexcept -> float { return max_score_; }

   private:
    scored_range_type scored_range_;
    float max_score_;
};

template <typename Frequency_Cursor, typename Term_Scorer, typename Wand_Cursor>
class Block_Max_Scored_Cursor {
   public:
    using scored_cursor_type = Scored_Cursor<Frequency_Cursor, Term_Scorer>;
    using document_type = uint32_t;

    Block_Max_Scored_Cursor(scored_cursor_type &&scored_cursor,
                            Wand_Cursor wand_cursor,
                            float term_weight,
                            float max_score)
        : scored_cursor_(std::forward<scored_cursor_type>(scored_cursor)),
          wand_cursor_(std::move(wand_cursor)),
          term_weight_(term_weight),
          max_score_(max_score)
    {}
    Block_Max_Scored_Cursor(Block_Max_Scored_Cursor const &) = default;
    Block_Max_Scored_Cursor(Block_Max_Scored_Cursor &&) noexcept(
        noexcept(std::move(std::declval<Frequency_Cursor>())) and
        noexcept(std::move(std::declval<Term_Scorer>()))) = default;
    Block_Max_Scored_Cursor &operator=(Block_Max_Scored_Cursor const &) = default;
    Block_Max_Scored_Cursor &operator=(Block_Max_Scored_Cursor &&) noexcept(
        noexcept_move_assignment<scored_cursor_type>()) = default;
    ~Block_Max_Scored_Cursor() = default;

    void reset() { scored_cursor_.reset(); }
    void DS2I_FLATTEN_FUNC next() { scored_cursor_.next(); }
    void DS2I_FLATTEN_FUNC next_geq(uint64_t lower_bound) { scored_cursor_.next_geq(lower_bound); }
    void DS2I_FLATTEN_FUNC move(uint64_t position) { scored_cursor_.move(position); }
    [[nodiscard]] auto docid() const { return scored_cursor_.docid(); }
    [[nodiscard]] auto score() { return scored_cursor_.score(); }
    [[nodiscard]] auto position() const { return scored_cursor_.position(); }
    [[nodiscard]] auto max_score() const -> float { return max_score_; }
    [[nodiscard]] auto block_max_score() -> float { return block_max_score(docid()); }
    [[nodiscard]] auto block_max_score(document_type id) -> float
    {
        if (wand_cursor_.docid() < id) {
            wand_cursor_.next_geq(id);
        }
        return wand_cursor_.score() * term_weight_;
    }
    [[nodiscard]] auto term_weight() const -> float { return term_weight_; }
    [[nodiscard]] auto block_docid() const -> uint32_t { return wand_cursor_.docid(); }

   private:
    scored_cursor_type scored_cursor_;
    Wand_Cursor wand_cursor_;
    float term_weight_;
    float max_score_;
};

template <typename Frequency_Range, typename Term_Scorer, typename Wand_Data>
class Block_Max_Scored_Range {
   public:
    using scored_range_type = Scored_Range<Frequency_Range, Term_Scorer>;
    using cursor_type = Block_Max_Scored_Cursor<typename Frequency_Range::cursor_type,
                                                Term_Scorer,
                                                typename Wand_Data::wand_data_enumerator>;
    using document_type = uint32_t;

    Block_Max_Scored_Range(scored_range_type &&scored_range,
                           Wand_Data *wand_data,
                           float term_weight,
                           float max_score,
                           uint32_t term_id)
        : scored_range_(std::forward<scored_range_type>(scored_range)),
          wand_data_(wand_data),
          term_weight_(term_weight),
          max_score_(max_score),
          term_(term_id)
    {}
    Block_Max_Scored_Range(Block_Max_Scored_Range const &) = delete;
    Block_Max_Scored_Range(Block_Max_Scored_Range &&) noexcept(
        noexcept(std::move(std::declval<Frequency_Range>())) and
        noexcept(std::move(std::declval<Term_Scorer>()))) = default;
    Block_Max_Scored_Range &operator=(Block_Max_Scored_Range const &) = delete;
    Block_Max_Scored_Range &operator=(Block_Max_Scored_Range &&) = default;
    ~Block_Max_Scored_Range() = default;

    [[nodiscard]] auto size() const -> int64_t { return scored_range_.size(); }
    [[nodiscard]] auto first_document() const { return scored_range_.first_document(); }
    [[nodiscard]] auto last_document() const { return scored_range_.last_document(); }
    [[nodiscard]] auto cursor() const -> cursor_type
    {
        return cursor_type{
            scored_range_.cursor(), wand_data_->getenum(term_), term_weight_, max_score_};
    }
    [[nodiscard]] auto operator()(document_type low, document_type hi) const
    {
        return Block_Max_Scored_Range(scored_range_(low, hi), wand_data_, term_weight_);
    }

   private:
    scored_range_type scored_range_;
    Wand_Data* wand_data_;
    float term_weight_;
    float max_score_;
    uint32_t term_;
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

template <typename Index, typename WandType>
[[nodiscard]] auto max_scored_ranges(Index const &index, WandType const &wdata, term_id_vec terms)
{
    using scorer_type = bm25;
    using range_type = typename Index::Posting_Range;
    using score_function_type = Score_Function<scorer_type, WandType>;
    using scored_range_type = Scored_Range<range_type, score_function_type>;
    using max_scored_range_type = Max_Scored_Range<range_type, score_function_type>;

    std::vector<max_scored_range_type> ranges;
    for (auto term : query_freqs(terms)) {
        auto freq_range = index.posting_range(term.first);
        auto q_weight =
            scorer_type::query_term_weight(term.second, freq_range.size(), index.num_docs());
        auto max_score = q_weight * wdata.max_term_weight(term.first);
        ranges.emplace_back(
            Scored_Range(std::move(freq_range), score_function_type{q_weight, std::cref(wdata)}),
            max_score);
    }
    return ranges;
}

template <typename Index, typename WandType>
[[nodiscard]] auto block_max_scored_ranges(Index const &index, WandType &wdata, term_id_vec terms)
{
    using scorer_type = bm25;
    using range_type = typename Index::Posting_Range;
    using score_function_type = Score_Function<scorer_type, WandType>;
    using scored_range_type = Scored_Range<range_type, score_function_type>;
    using block_max_scored_range_type =
        Block_Max_Scored_Range<range_type, score_function_type, WandType>;

    std::vector<block_max_scored_range_type> ranges;
    for (auto term : query_freqs(terms)) {
        auto freq_range = index.posting_range(term.first);
        auto q_weight =
            scorer_type::query_term_weight(term.second, freq_range.size(), index.num_docs());
        auto max_score = q_weight * wdata.max_term_weight(term.first);
        ranges.emplace_back(
            Scored_Range(std::move(freq_range), score_function_type{q_weight, std::cref(wdata)}),
            &wdata,
            q_weight,
            max_score,
            term.first);
    }
    return ranges;
}

}; // namespace pisa
