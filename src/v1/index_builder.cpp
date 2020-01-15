#include <algorithm>
#include <cstddef>
#include <functional>

#include <boost/filesystem.hpp>
#include <gsl/span>
#include <mio/mmap.hpp>
#include <range/v3/view/transform.hpp>
#include <tbb/task_group.h>

#include "codec/simdbp.hpp"
#include "v1/blocked_cursor.hpp"
#include "v1/cursor/accumulate.hpp"
#include "v1/cursor_intersection.hpp"
#include "v1/default_index_runner.hpp"
#include "v1/index_builder.hpp"
#include "v1/index_metadata.hpp"
#include "v1/io.hpp"
#include "v1/query.hpp"
#include "v1/scorer/bm25.hpp"

namespace pisa::v1 {

[[nodiscard]] auto make_temp() -> std::string
{
    return (boost::filesystem::temp_directory_path() / boost::filesystem::unique_path()).string();
}

template <typename Index>
void write_document_header(Index&& index, std::ostream& os)
{
    using index_type = std::decay_t<Index>;
    using writer_type = typename CursorTraits<typename index_type::document_cursor_type>::Writer;
    PostingBuilder<DocId>(writer_type{index.num_documents()}).write_header(os);
}

template <typename Index>
void write_payload_header(Index&& index, std::ostream& os)
{
    using index_type = std::decay_t<Index>;
    using writer_type = typename CursorTraits<typename index_type::payload_cursor_type>::Writer;
    using value_type = typename CursorTraits<typename index_type::payload_cursor_type>::Value;
    PostingBuilder<value_type>(writer_type{index.num_documents()}).write_header(os);
}

auto merge_into(PostingFilePaths const& batch,
                std::ofstream& posting_sink,
                std::ofstream& offsets_sink,
                std::size_t shift) -> std::size_t
{
    std::ifstream batch_stream(batch.postings);
    batch_stream.seekg(0, std::ios::end);
    std::streamsize size = batch_stream.tellg();
    batch_stream.seekg(0, std::ios::beg);
    posting_sink << batch_stream.rdbuf();
    auto offsets = load_vector<std::size_t>(batch.offsets);
    std::transform(offsets.begin(), offsets.end(), offsets.begin(), [shift](auto offset) {
        return shift + offset;
    });

    // Because each batch has one superfluous offset indicating the end of data.
    // Effectively, the last offset of batch `i` overlaps with the first offsets of batch `i + 1`.
    std::size_t start = shift > 0 ? 8U : 0U;

    offsets_sink.write(reinterpret_cast<char const*>(offsets.data()) + start,
                       offsets.size() * sizeof(std::size_t) - start);
    return shift + size;
}

template <typename Rng>
void merge_postings(std::string const& message,
                    std::ofstream& posting_sink,
                    std::ofstream& offset_sink,
                    Rng&& batches)
{
    ProgressStatus status(
        batches.size(), DefaultProgressCallback(message), std::chrono::milliseconds(500));
    std::size_t shift = 0;
    for (auto&& batch : batches) {
        shift = merge_into(batch, posting_sink, offset_sink, shift);
        boost::filesystem::remove(batch.postings);
        boost::filesystem::remove(batch.offsets);
        status += 1;
    };
}

/// Represents open-ended interval [begin, end).
template <typename T>
struct Interval {
    Interval(std::size_t begin, std::size_t end, gsl::span<T> const& elements)
        : m_begin(begin), m_end(end), m_elements(elements)
    {
    }
    [[nodiscard]] auto span() const -> gsl::span<T>
    {
        return m_elements.subspan(begin(), end() - begin());
    }
    [[nodiscard]] auto begin() const -> std::size_t { return m_begin; }
    [[nodiscard]] auto end() const -> std::size_t { return m_end; }

   private:
    std::size_t m_begin;
    std::size_t m_end;
    gsl::span<T> const& m_elements;
};

template <typename Element>
struct BatchConcurrentBuilder {
    explicit BatchConcurrentBuilder(std::size_t num_batches, gsl::span<Element const> elements)
        : m_num_batches(num_batches), m_elements(elements)
    {
        runtime_assert(elements.size() >= num_batches).or_exit([&] {
            return fmt::format(
                "The number of elements ({}) must be at least the number of batches ({})",
                elements.size(),
                num_batches);
        });
    }

    template <typename BatchFn>
    void execute_batch_jobs(BatchFn batch_job)
    {
        auto batch_size = m_elements.size() / m_num_batches;
        tbb::task_group group;
        for (auto batch = 0; batch < m_num_batches; batch += 1) {
            group.run([batch_size, batch_job, batch, this] {
                auto first_idx = batch * batch_size;
                auto last_idx = batch < this->m_num_batches - 1 ? (batch + 1) * batch_size
                                                                : this->m_elements.size();
                batch_job(batch, Interval<Element const>(first_idx, last_idx, m_elements));
            });
        }
        group.wait();
    }

   private:
    std::size_t m_num_batches;
    gsl::span<Element const> m_elements;
};

auto collect_unique_bigrams(std::vector<Query> const& queries,
                            std::function<void()> const& callback)
    -> std::vector<std::pair<TermId, TermId>>
{
    std::vector<std::pair<TermId, TermId>> bigrams;
    auto idx = 0;
    for (auto const& query : queries) {
        auto const& term_ids = query.get_term_ids();
        if (term_ids.empty()) {
            continue;
        }
        callback();
        for (auto left = 0; left < term_ids.size() - 1; left += 1) {
            for (auto right = left + 1; right < term_ids.size(); right += 1) {
                bigrams.emplace_back(term_ids[left], term_ids[right]);
            }
        }
    }
    std::sort(bigrams.begin(), bigrams.end());
    bigrams.erase(std::unique(bigrams.begin(), bigrams.end()), bigrams.end());
    return bigrams;
}

auto verify_compressed_index(std::string const& input, std::string_view output)
    -> std::vector<std::string>
{
    std::vector<std::string> errors;
    pisa::binary_freq_collection const collection(input.c_str());
    auto meta = IndexMetadata::from_file(fmt::format("{}.yml", output));
    auto run = index_runner(meta);
    ProgressStatus status(
        collection.size(), DefaultProgressCallback("Verifying"), std::chrono::milliseconds(500));
    run([&](auto&& index) {
        auto sequence_iter = collection.begin();
        for (auto term = 0; term < index.num_terms(); term += 1, ++sequence_iter) {
            auto document_sequence = sequence_iter->docs;
            auto frequency_sequence = sequence_iter->freqs;
            auto cursor = index.cursor(term);
            if (cursor.size() != document_sequence.size()) {
                errors.push_back(
                    fmt::format("Posting list length mismatch for term {}: expected {} but is {}",
                                term,
                                document_sequence.size(),
                                cursor.size()));
                continue;
            }
            auto dit = document_sequence.begin();
            auto fit = frequency_sequence.begin();
            auto pos = 0;
            while (not cursor.empty()) {
                if (cursor.value() != *dit) {
                    errors.push_back(
                        fmt::format("Document mismatch for term {} at position {}", term, pos));
                }
                if (cursor.payload() != *fit) {
                    errors.push_back(
                        fmt::format("Frequency mismatch for term {} at position {}: {} != {}",
                                    term,
                                    pos,
                                    cursor.payload(),
                                    *fit));
                }
                cursor.advance();
                ++dit;
                ++fit;
                ++pos;
            }
            status += 1;
        }
    });
    return errors;
}

[[nodiscard]] auto build_scored_pair_index(IndexMetadata meta,
                                           std::string const& index_basename,
                                           std::vector<std::pair<TermId, TermId>> const& pairs,
                                           std::size_t threads)
    -> std::pair<PostingFilePaths, PostingFilePaths>
{
    auto run = scored_index_runner(std::move(meta));

    PostingFilePaths scores_0 = {.postings = fmt::format("{}.bigram_bm25_0", index_basename),
                                 .offsets =
                                     fmt::format("{}.bigram_bm25_offsets_0", index_basename)};
    PostingFilePaths scores_1 = {.postings = fmt::format("{}.bigram_bm25_1", index_basename),
                                 .offsets =
                                     fmt::format("{}.bigram_bm25_offsets_1", index_basename)};
    std::ofstream score_out_0(scores_0.postings);
    std::ofstream score_out_1(scores_1.postings);

    ProgressStatus status(pairs.size(),
                          DefaultProgressCallback("Building scored pair index"),
                          std::chrono::milliseconds(500));

    std::vector<std::array<PostingFilePaths, 2>> batch_files(threads);

    BatchConcurrentBuilder batch_builder(threads,
                                         gsl::span<std::pair<TermId, TermId> const>(pairs));
    run([&](auto&& index) {
        using index_type = std::decay_t<decltype(index)>;
        using score_writer_type =
            typename CursorTraits<typename index_type::payload_cursor_type>::Writer;

        batch_builder.execute_batch_jobs(
            [&status, &batch_files, &index](auto batch_idx, auto interval) {
                auto scores_file_0 = make_temp();
                auto scores_file_1 = make_temp();
                auto score_offsets_file_0 = make_temp();
                auto score_offsets_file_1 = make_temp();
                std::ofstream score_out_0(scores_file_0);
                std::ofstream score_out_1(scores_file_1);

                PostingBuilder<std::uint8_t> score_builder_0(score_writer_type{});
                PostingBuilder<std::uint8_t> score_builder_1(score_writer_type{});

                for (auto [left_term, right_term] : interval.span()) {
                    auto intersection = intersect({index.scored_cursor(left_term, VoidScorer{}),
                                                   index.scored_cursor(right_term, VoidScorer{})},
                                                  std::array<std::uint8_t, 2>{0, 0},
                                                  [](auto& payload, auto& cursor, auto list_idx) {
                                                      gsl::at(payload, list_idx) = cursor.payload();
                                                      return payload;
                                                  });
                    if (intersection.empty()) {
                        status += 1;
                        continue;
                    }
                    for_each(intersection, [&](auto& cursor) {
                        auto payload = cursor.payload();
                        score_builder_0.accumulate(std::get<0>(payload));
                        score_builder_1.accumulate(std::get<1>(payload));
                    });
                    score_builder_0.flush_segment(score_out_0);
                    score_builder_1.flush_segment(score_out_1);
                    status += 1;
                }
                write_span(gsl::make_span(score_builder_0.offsets()), score_offsets_file_0);
                write_span(gsl::make_span(score_builder_1.offsets()), score_offsets_file_1);
                batch_files[batch_idx] = {PostingFilePaths{scores_file_0, score_offsets_file_0},
                                          PostingFilePaths{scores_file_1, score_offsets_file_1}};
            });

        write_payload_header(index, score_out_0);
        write_payload_header(index, score_out_1);
    });

    std::ofstream score_offsets_out_0(scores_0.offsets);
    std::ofstream score_offsets_out_1(scores_1.offsets);

    merge_postings(
        "Merging scores<0>",
        score_out_0,
        score_offsets_out_0,
        ranges::views::transform(batch_files, [](auto&& files) { return std::get<0>(files); }));

    merge_postings(
        "Merging scores<1>",
        score_out_1,
        score_offsets_out_1,
        ranges::views::transform(batch_files, [](auto&& files) { return std::get<1>(files); }));

    return {scores_0, scores_1};
}

[[nodiscard]] auto build_scored_bigram_index(IndexMetadata meta,
                                             std::string const& index_basename,
                                             std::vector<std::pair<TermId, TermId>> const& bigrams)
    -> std::pair<PostingFilePaths, PostingFilePaths>
{
    auto run = scored_index_runner(std::move(meta));

    auto scores_file_0 = fmt::format("{}.bigram_bm25_0", index_basename);
    auto scores_file_1 = fmt::format("{}.bigram_bm25_1", index_basename);
    auto score_offsets_file_0 = fmt::format("{}.bigram_bm25_offsets_0", index_basename);
    auto score_offsets_file_1 = fmt::format("{}.bigram_bm25_offsets_1", index_basename);
    std::ofstream score_out_0(scores_file_0);
    std::ofstream score_out_1(scores_file_1);

    run([&](auto&& index) {
        ProgressStatus status(bigrams.size(),
                              DefaultProgressCallback("Building scored index"),
                              std::chrono::milliseconds(500));
        using index_type = std::decay_t<decltype(index)>;
        using score_writer_type =
            typename CursorTraits<typename index_type::payload_cursor_type>::Writer;

        PostingBuilder<std::uint8_t> score_builder_0(score_writer_type{});
        PostingBuilder<std::uint8_t> score_builder_1(score_writer_type{});
        PostingBuilder<std::uint8_t>(score_writer_type{});

        score_builder_0.write_header(score_out_0);
        score_builder_1.write_header(score_out_1);

        for (auto [left_term, right_term] : bigrams) {
            auto intersection = intersect({index.scored_cursor(left_term, VoidScorer{}),
                                           index.scored_cursor(right_term, VoidScorer{})},
                                          std::array<std::uint8_t, 2>{0, 0},
                                          [](auto& payload, auto& cursor, auto list_idx) {
                                              gsl::at(payload, list_idx) = cursor.payload();
                                              return payload;
                                          });
            if (intersection.empty()) {
                status += 1;
                continue;
            }
            for_each(intersection, [&](auto& cursor) {
                auto payload = cursor.payload();
                score_builder_0.accumulate(std::get<0>(payload));
                score_builder_1.accumulate(std::get<1>(payload));
            });
            score_builder_0.flush_segment(score_out_0);
            score_builder_1.flush_segment(score_out_1);
            status += 1;
        }
        write_span(gsl::make_span(score_builder_0.offsets()), score_offsets_file_0);
        write_span(gsl::make_span(score_builder_1.offsets()), score_offsets_file_1);
    });
    return {PostingFilePaths{scores_file_0, score_offsets_file_0},
            PostingFilePaths{scores_file_1, score_offsets_file_1}};
}

template <typename T, typename Order = std::less<>>
struct HeapPriorityQueue {
    using value_type = T;

    explicit HeapPriorityQueue(std::size_t capacity, Order order = Order())
        : m_capacity(capacity), m_order(std::move(order))
    {
        m_elements.reserve(m_capacity + 1);
    }
    HeapPriorityQueue(HeapPriorityQueue const&) = default;
    HeapPriorityQueue(HeapPriorityQueue&&) noexcept = default;
    HeapPriorityQueue& operator=(HeapPriorityQueue const&) = default;
    HeapPriorityQueue& operator=(HeapPriorityQueue&&) noexcept = default;
    ~HeapPriorityQueue() = default;

    void push(value_type value)
    {
        m_elements.push_back(value);
        std::push_heap(m_elements.begin(), m_elements.end(), m_order);
        if (PISA_LIKELY(m_elements.size() > m_capacity)) {
            std::pop_heap(m_elements.begin(), m_elements.end(), m_order);
            m_elements.pop_back();
        }
    }

    [[nodiscard]] auto size() const noexcept { return m_elements.size(); }
    [[nodiscard]] auto capacity() const noexcept { return m_capacity; }

    [[nodiscard]] auto take() && -> std::vector<value_type>
    {
        std::sort(m_elements.begin(), m_elements.end(), m_order);
        return std::move(m_elements);
    }

   private:
    std::size_t m_capacity;
    std::vector<value_type> m_elements;
    Order m_order;
};

[[nodiscard]] auto select_best_bigrams(IndexMetadata const& meta,
                                       std::vector<Query> const& queries,
                                       std::size_t num_bigrams_to_select)
    -> std::vector<std::pair<TermId, TermId>>
{
    using Bigram = std::pair<Query const*, float>;
    auto order = [](auto&& lhs, auto&& rhs) { return lhs.second > rhs.second; };
    auto top_bigrams = HeapPriorityQueue<Bigram, decltype(order)>(num_bigrams_to_select, order);

    auto run = index_runner(meta);
    run([&](auto&& index) {
        auto bigram_gain = [&](Query const& bigram) -> float {
            auto&& term_ids = bigram.get_term_ids();
            runtime_assert(term_ids.size() == 2)
                .or_throw("Queries must be of exactly two unique terms");
            auto cursors = index.scored_cursors(term_ids, make_bm25(index));
            auto union_length = cursors[0].size() + cursors[1].size();
            auto intersection_length =
                accumulate(intersect(std::move(cursors),
                                     false,
                                     []([[maybe_unused]] auto count,
                                        [[maybe_unused]] auto&& cursor,
                                        [[maybe_unused]] auto idx) { return true; }),
                           std::size_t{0},
                           [](auto count, [[maybe_unused]] auto&& cursor) { return count + 1; });
            if (intersection_length == 0) {
                return 0.0;
            }
            return static_cast<double>(bigram.get_probability()) * static_cast<double>(union_length)
                   / static_cast<double>(intersection_length);
        };
        for (auto&& query : queries) {
            auto&& term_ids = query.get_term_ids();
            top_bigrams.push(std::make_pair(&query, bigram_gain(query)));
        }
    });
    auto top = std::move(top_bigrams).take();
    return ranges::views::transform(top,
                                    [](auto&& elem) {
                                        auto&& term_ids = elem.first->get_term_ids();
                                        return std::make_pair(term_ids[0], term_ids[1]);
                                    })
           | ranges::to_vector;
}

template <typename Index>
auto build_pair_batch(Index&& index,
                      gsl::span<std::pair<TermId, TermId> const> pairs,
                      ProgressStatus& status)
    -> std::pair<BigramMetadata, std::vector<std::array<TermId, 2>>>
{
    using index_type = std::decay_t<decltype(index)>;
    using document_writer_type =
        typename CursorTraits<typename index_type::document_cursor_type>::Writer;
    using frequency_writer_type =
        typename CursorTraits<typename index_type::payload_cursor_type>::Writer;

    std::vector<std::array<TermId, 2>> pair_mapping;

    BigramMetadata batch_meta{.documents = {.postings = make_temp(), .offsets = make_temp()},
                              .frequencies = {{.postings = make_temp(), .offsets = make_temp()},
                                              {.postings = make_temp(), .offsets = make_temp()}}};
    std::ofstream document_out(batch_meta.documents.postings);
    std::ofstream frequency_out_0(batch_meta.frequencies.first.postings);
    std::ofstream frequency_out_1(batch_meta.frequencies.second.postings);

    PostingBuilder<DocId> document_builder(document_writer_type{index.num_documents()});
    PostingBuilder<Frequency> frequency_builder_0(frequency_writer_type{index.num_documents()});
    PostingBuilder<Frequency> frequency_builder_1(frequency_writer_type{index.num_documents()});

    for (auto [left_term, right_term] : pairs) {
        auto intersection = intersect({index.cursor(left_term), index.cursor(right_term)},
                                      std::array<Frequency, 2>{0, 0},
                                      [](auto& payload, auto& cursor, auto list_idx) {
                                          gsl::at(payload, list_idx) = cursor.payload();
                                          return payload;
                                      });
        if (intersection.empty()) {
            status += 1;
            continue;
        }
        pair_mapping.push_back({left_term, right_term});
        for_each(intersection, [&](auto& cursor) {
            document_builder.accumulate(*cursor);
            auto payload = cursor.payload();
            frequency_builder_0.accumulate(std::get<0>(payload));
            frequency_builder_1.accumulate(std::get<1>(payload));
        });
        document_builder.flush_segment(document_out);
        frequency_builder_0.flush_segment(frequency_out_0);
        frequency_builder_1.flush_segment(frequency_out_1);
        status += 1;
    }
    write_span(gsl::make_span(document_builder.offsets()), batch_meta.documents.offsets);
    write_span(gsl::make_span(frequency_builder_0.offsets()), batch_meta.frequencies.first.offsets);
    write_span(gsl::make_span(frequency_builder_1.offsets()),
               batch_meta.frequencies.second.offsets);
    return {std::move(batch_meta), std::move(pair_mapping)};
}

auto build_pair_index(IndexMetadata meta,
                      std::vector<std::pair<TermId, TermId>> const& pairs,
                      tl::optional<std::string> const& clone_path,
                      std::size_t threads) -> IndexMetadata
{
    runtime_assert(not pairs.empty()).or_throw("Pair index must contain pairs but none passed");
    std::string index_basename = clone_path.value_or(std::string(meta.get_basename()));
    auto run = index_runner(meta);

    std::vector<std::vector<std::array<TermId, 2>>> pair_mapping(threads);
    std::vector<BigramMetadata> batch_meta(threads);

    PostingFilePaths documents{.postings = fmt::format("{}.bigram_documents", index_basename),
                               .offsets =
                                   fmt::format("{}.bigram_document_offsets", index_basename)};
    PostingFilePaths frequencies_0{
        .postings = fmt::format("{}.bigram_frequencies_0", index_basename),
        .offsets = fmt::format("{}.bigram_frequency_offsets_0", index_basename)};
    PostingFilePaths frequencies_1{
        .postings = fmt::format("{}.bigram_frequencies_1", index_basename),
        .offsets = fmt::format("{}.bigram_frequency_offsets_1", index_basename)};

    std::ofstream document_out(documents.postings);
    std::ofstream frequency_out_0(frequencies_0.postings);
    std::ofstream frequency_out_1(frequencies_1.postings);

    std::ofstream document_offsets_out(documents.offsets);
    std::ofstream frequency_offsets_out_0(frequencies_0.offsets);
    std::ofstream frequency_offsets_out_1(frequencies_1.offsets);

    ProgressStatus status(pairs.size(),
                          DefaultProgressCallback("Building bigram index"),
                          std::chrono::milliseconds(500));

    BatchConcurrentBuilder batch_builder(threads,
                                         gsl::span<std::pair<TermId, TermId> const>(pairs));
    run([&](auto&& index) {
        using index_type = std::decay_t<decltype(index)>;
        using document_writer_type =
            typename CursorTraits<typename index_type::document_cursor_type>::Writer;
        using frequency_writer_type =
            typename CursorTraits<typename index_type::payload_cursor_type>::Writer;

        batch_builder.execute_batch_jobs([&](auto batch_idx, auto interval) {
            auto res = build_pair_batch(index, interval.span(), status);
            batch_meta[batch_idx] = res.first;
            pair_mapping[batch_idx] = std::move(res.second);
        });

        write_document_header(index, document_out);
        write_payload_header(index, frequency_out_0);
        write_payload_header(index, frequency_out_1);
    });

    merge_postings(
        "Merging documents",
        document_out,
        document_offsets_out,
        ranges::views::transform(batch_meta, [](auto&& meta) { return meta.documents; }));

    merge_postings(
        "Merging frequencies<0>",
        frequency_out_0,
        frequency_offsets_out_0,
        ranges::views::transform(batch_meta, [](auto&& meta) { return meta.frequencies.first; }));

    merge_postings(
        "Merging frequencies<1>",
        frequency_out_1,
        frequency_offsets_out_1,
        ranges::views::transform(batch_meta, [](auto&& meta) { return meta.frequencies.second; }));

    auto count = std::accumulate(pair_mapping.begin(),
                                 pair_mapping.end(),
                                 std::size_t{0},
                                 [](auto acc, auto&& m) { return acc + m.size(); });
    BigramMetadata bigram_meta{.documents = documents,
                               .frequencies = {frequencies_0, frequencies_1},
                               .scores = {},
                               .mapping = fmt::format("{}.bigram_mapping", index_basename),
                               .count = count};

    if (not meta.scores.empty()) {
        bigram_meta.scores.push_back(build_scored_pair_index(meta, index_basename, pairs, threads));
    }
    meta.bigrams = bigram_meta;

    std::cerr << "Writing metadata...";
    if (clone_path) {
        meta.write(append_extension(clone_path.value()));
    } else {
        meta.update();
    }
    std::cerr << " Done.\nWriting bigram mapping...";
    std::ofstream os(meta.bigrams->mapping);
    for (auto mapping_batch : pair_mapping) {
        write_span(gsl::make_span(mapping_batch), os);
    }
    std::cerr << " Done.\n";
    return meta;
}

} // namespace pisa::v1
