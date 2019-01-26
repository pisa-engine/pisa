#pragma once

#include <algorithm>
#include <atomic>
#include <fstream>
#include <functional>
#include <iostream>
#include <numeric>
#include <optional>
#include <thread>
#include <unordered_map>
#include <sstream>

#include "gsl/span"
#include "boost/filesystem.hpp"
#include "pstl/algorithm"
#include "pstl/execution"
#include "spdlog/spdlog.h"
#include "tbb/concurrent_queue.h"
#include "tbb/task_group.h"

#include "binary_collection.hpp"
#include "enumerate.hpp"
#include "util/util.hpp"

namespace pisa {

template <class Tag, class T, T default_value = 0>
class Integer {
   public:
    Integer() : m_val(default_value) {}
    explicit Integer(T val) : m_val(val) {}
    Integer(Integer const &) = default;
    Integer(Integer &&) noexcept = default;
    Integer &operator=(Integer const &) = default;
    Integer &operator=(Integer &&) noexcept = default;

    explicit operator T() const { return m_val; }
    [[nodiscard]] T get() const { return m_val; }

    [[nodiscard]] bool operator==(Integer const &other) const { return m_val == other.m_val; }
    [[nodiscard]] bool operator!=(Integer const &other) const { return m_val != other.m_val; }
    [[nodiscard]] bool operator<(Integer const &other) const { return m_val < other.m_val; }
    [[nodiscard]] bool operator<=(Integer const &other) const { return m_val <= other.m_val; }
    [[nodiscard]] bool operator>(Integer const &other) const { return m_val > other.m_val; }
    [[nodiscard]] bool operator>=(Integer const &other) const { return m_val >= other.m_val; }

    Integer &operator++() {
        ++m_val;
        return *this;
    }

    [[nodiscard]] Integer operator+(T difference) const { return Integer(m_val + difference); }
    Integer &             operator+=(T difference) {
        m_val += difference;
        return *this;
    }
    [[nodiscard]] Integer operator+(Integer const &other) const {
        return Integer(m_val + other.m_val);
    }
    Integer &operator+=(Integer const &other) {
        m_val += other.m_val;
        return *this;
    }
    [[nodiscard]] Integer operator-(Integer const &other) const {
        return Integer(m_val - other.m_val);
    }
    Integer &operator-=(Integer const &other) {
        m_val -= other.m_val;
        return *this;
    }

   private:
    T m_val;
};

} // namespace pisa

namespace std {

template <class Tag, class T, T default_value>
struct hash<pisa::Integer<Tag, T, default_value>> {
    constexpr auto operator()(pisa::Integer<Tag, T, default_value> const &key) const noexcept {
        return hash<T>{}(static_cast<T>(key));
    }
};

} // namespace std

namespace pisa {

template <class Tag, class T, T default_value>
std::ostream &operator<<(std::ostream &os, Integer<Tag, T, default_value> id) {
    return os << static_cast<T>(id);
}

struct document_id_tag {};
using Document_Id = Integer<document_id_tag, std::int32_t>;
struct term_id_tag {};
using Term_Id = Integer<term_id_tag, std::int32_t>;
struct frequency_tag {};
using Frequency = Integer<frequency_tag, std::int32_t>;

namespace literals {

inline Document_Id operator"" _d(unsigned long long n)  // NOLINT
{
    return Document_Id(n);
}

inline Term_Id operator"" _t(unsigned long long n)  // NOLINT
{
    return Term_Id(n);
}

inline Frequency operator"" _f(unsigned long long n)  // NOLINT
{
    return Frequency(n);
}

} // namespace literals

template <typename T>
std::vector<T> concatenate(std::vector<std::vector<T>> const &containers)
{
    auto full_size = std::accumulate(
        containers.begin(), containers.end(), 0, [](auto const &acc, auto const &container) {
            return acc + container.size();
        });
    std::vector<T> vec(full_size);
    auto next_begin = std::begin(vec);
    for (auto const& container : containers) {
        std::copy(container.begin(), container.end(), next_begin);
        next_begin = std::next(next_begin, container.size());
    }
    return vec;
}

template <typename T>
static std::ostream &write_sequence(std::ostream &os, gsl::span<T> sequence)
{
    auto length = static_cast<uint32_t>(sequence.size());
    os.write(reinterpret_cast<const char *>(&length), sizeof(length));
    os.write(reinterpret_cast<const char *>(sequence.data()), length * sizeof(T));
    return os;
}

namespace invert {

struct Batch {
    gsl::span<gsl::span<Term_Id const>> documents;
    Enumerator_Range<Document_Id>       document_ids;
};

std::vector<std::pair<Term_Id, Document_Id>> map_to_postings(Batch batch)
{
    auto docid = batch.document_ids.begin();
    std::vector<std::pair<Term_Id, Document_Id>> postings;
    for (auto const &document : batch.documents) {
        for (auto const &term : document) {
            postings.emplace_back(term, *docid);
        }
        ++docid;
    }
    return postings;
}

void join_term(std::vector<Document_Id> &lower_doc,
               std::vector<Frequency> &lower_freq,
               std::vector<Document_Id> &higher_doc,
               std::vector<Frequency> &higher_freq)
{
    if (lower_doc.back() == higher_doc.front()) {
        lower_freq.back() += higher_freq.front();
        lower_doc.insert(lower_doc.end(), std::next(higher_doc.begin()), higher_doc.end());
        lower_freq.insert(lower_freq.end(), std::next(higher_freq.begin()), higher_freq.end());
    } else {
        lower_doc.insert(lower_doc.end(), higher_doc.begin(), higher_doc.end());
        lower_freq.insert(lower_freq.end(), higher_freq.begin(), higher_freq.end());
    }
}

template <typename Iterator>
struct Inverted_Index {
    using iterator_type = Iterator;
    std::unordered_map<Term_Id, std::vector<Document_Id>> documents{};
    std::unordered_map<Term_Id, std::vector<Frequency>>   frequencies{};

    Inverted_Index() = default;
    Inverted_Index(Inverted_Index &, tbb::split) {}
    Inverted_Index(std::unordered_map<Term_Id, std::vector<Document_Id>> documents,
                   std::unordered_map<Term_Id, std::vector<Frequency>>   frequencies)
        : documents(std::move(documents)), frequencies(std::move(frequencies)) {}

    void operator()(tbb::blocked_range<iterator_type> const &r) {
        if (auto first = r.begin(); first != r.end()) {
            if (auto current_term = first->first; not documents[current_term].empty()) {
                auto current_doc = documents[current_term].back();
                auto &freq = frequencies[current_term].back();
                auto last = std::find_if(first, r.end(), [&](auto const &posting) {
                    return posting.first != current_term || posting.second != current_doc;
                });
                freq += Frequency(std::distance(first, last));
                first = last;
            }
            while (first != r.end()) {
                auto [current_term, current_doc] = *first;
                auto last = std::find_if(first, r.end(), [&, current_term = current_term, current_doc = current_doc](auto const &posting) {
                    return posting.first != current_term || posting.second != current_doc;
                });
                auto freq = Frequency(std::distance(first, last));
                documents[current_term].push_back(current_doc);
                frequencies[current_term].push_back(freq);
                first = last;
            }
        }
    }

    void join(Inverted_Index &rhs) {
        for (auto &&[term_id, document_ids] : rhs.documents) {
            if (auto pos = documents.find(term_id); pos == documents.end()) {
                std::swap(documents[term_id], document_ids);
                std::swap(frequencies[term_id], rhs.frequencies[term_id]);
            } else if (pos->second.back() <= document_ids.front()) {
                join_term(
                    pos->second, frequencies[term_id], document_ids, rhs.frequencies[term_id]);
            } else {
                join_term(
                    document_ids, rhs.frequencies[term_id], pos->second, frequencies[term_id]);
                std::swap(documents[term_id], document_ids);
                std::swap(frequencies[term_id], rhs.frequencies[term_id]);
            }
        }
    }
};

template <typename Iterator>
void write(std::string const &                     basename,
           invert::Inverted_Index<Iterator> const &index,
           std::uint32_t                           term_count)
{
    std::ofstream dstream(basename + ".docs");
    std::ofstream fstream(basename + ".freqs");
    std::uint32_t count = index.documents.size();
    write_sequence(dstream, gsl::make_span<uint32_t const>(&count, 1));
    for (auto term : enumerate(Term_Id(term_count))) {
        if (auto pos = index.documents.find(term); pos != index.documents.end()) {
            auto const &documents = pos->second;
            auto const &frequencies = index.frequencies.at(term);
            write_sequence(dstream, gsl::span<Document_Id const>(documents));
            write_sequence(fstream, gsl::span<Frequency const>(frequencies));
        } else {
            write_sequence(dstream, gsl::span<Document_Id const>());
            write_sequence(fstream, gsl::span<Frequency const>());
        }
    }
}

auto invert_range(gsl::span<gsl::span<Term_Id const>> documents,
                  Document_Id                         first_document_id,
                  size_t                              threads)
{
    gsl::index batch_size = (documents.size() + threads - 1) / threads;
    std::vector<Batch> batches;
    for (gsl::index first_idx_in_batch = 0; first_idx_in_batch < documents.size();
         first_idx_in_batch += batch_size)
    {
        auto last_idx_in_batch = std::min(first_idx_in_batch + batch_size, documents.size());
        auto first_document_in_batch = first_document_id + first_idx_in_batch;
        auto last_document_in_batch  = first_document_id + last_idx_in_batch;
        auto current_batch_size  = last_idx_in_batch - first_idx_in_batch;
        batches.push_back(Batch{documents.subspan(first_idx_in_batch, current_batch_size),
                                enumerate(first_document_in_batch, last_document_in_batch)});
    }
    std::vector<std::vector<std::pair<Term_Id, Document_Id>>> posting_vectors(batches.size());
    std::transform(std::execution::par_unseq,
                   batches.begin(),
                   batches.end(),
                   std::begin(posting_vectors),
                   map_to_postings);
    auto postings = concatenate(posting_vectors);
    posting_vectors.clear();
    posting_vectors.shrink_to_fit();

    std::sort(std::execution::par_unseq, postings.begin(), postings.end());

    using iterator_type = decltype(postings.begin());
    invert::Inverted_Index<iterator_type> index;
    tbb::parallel_reduce(tbb::blocked_range(postings.begin(), postings.end()), index);
    return index;
}

[[nodiscard]] auto build_batches(std::string const &input_basename,
                                 std::string const &output_basename,
                                 uint32_t           term_count,
                                 size_t             batch_size,
                                 size_t             threads) -> uint32_t
{
    uint32_t          batch = 0;
    binary_collection coll(input_basename.c_str());
    auto              doc_iter            = ++coll.begin();
    uint32_t          documents_processed = 0;
    while (doc_iter != coll.end()) {
        std::vector<gsl::span<Term_Id const>> documents;
        for (; doc_iter != coll.end() && documents.size() < batch_size; ++doc_iter) {
            auto document_sequence = *doc_iter;
            documents.emplace_back(reinterpret_cast<Term_Id const *>(document_sequence.begin()),
                                   document_sequence.size());
        }
        spdlog::info(
            "Inverting [{}, {})", documents_processed, documents_processed + documents.size());
        auto index = invert_range(documents, Document_Id(documents_processed), threads);

        std::ostringstream batch_name_stream;
        batch_name_stream << output_basename << ".batch." << batch;
        write(batch_name_stream.str(), index, term_count);

        documents_processed += documents.size();
        batch += 1;
    }
    return batch;
}

void merge_batches(std::string const &output_basename, uint32_t batch_count, uint32_t term_count)
{
    std::vector<binary_collection> doc_collections;
    std::vector<binary_collection> freq_collections;
    for (auto batch : enumerate(batch_count)) {
        std::ostringstream batch_name_stream;
        batch_name_stream << output_basename << ".batch." << batch;
        doc_collections.emplace_back((batch_name_stream.str() + ".docs").c_str());
        freq_collections.emplace_back((batch_name_stream.str() + ".freqs").c_str());
    }

    std::vector<binary_collection::const_iterator> doc_iterators;
    std::vector<binary_collection::const_iterator> freq_iterators;
    std::transform(doc_collections.begin(),
                   doc_collections.end(),
                   std::back_inserter(doc_iterators),
                   [](auto const &coll) { return ++coll.begin(); });
    std::transform(freq_collections.begin(),
                   freq_collections.end(),
                   std::back_inserter(freq_iterators),
                   [](auto const &coll) { return coll.begin(); });

    std::ofstream dos(output_basename + ".docs");
    std::ofstream fos(output_basename + ".freqs");
    write_sequence(dos, gsl::make_span<uint32_t const>(&term_count, 1));
    for ([[maybe_unused]] auto _ : enumerate(term_count)) {
        std::vector<uint32_t> dlist;
        for (auto &iter : doc_iterators) {
            auto seq = *iter;
            dlist.insert(dlist.end(), seq.begin(), seq.end());
            ++iter;
        }
        std::vector<uint32_t> flist;
        for (auto &iter : freq_iterators) {
            auto seq = *iter;
            flist.insert(flist.end(), seq.begin(), seq.end());
            ++iter;
        }
        write_sequence(dos, gsl::span<uint32_t const>(dlist));
        write_sequence(fos, gsl::span<uint32_t const>(flist));
    }
}

void invert_forward_index(std::string const &input_basename,
                          std::string const &output_basename,
                          uint32_t           term_count,
                          size_t             batch_size,
                          size_t             threads)
{
    uint32_t batch_count =
        invert::build_batches(input_basename, output_basename, term_count, batch_size, threads);
    invert::merge_batches(output_basename, batch_count, term_count);

    for (auto batch : enumerate(batch_count)) {
        std::ostringstream batch_name_stream;
        batch_name_stream << output_basename << ".batch." << batch;
        auto batch_basename = batch_name_stream.str();
        boost::filesystem::remove(boost::filesystem::path{batch_basename + ".docs"});
        boost::filesystem::remove(boost::filesystem::path{batch_basename + ".freqs"});
    }
}

} // namespace invert

} // namespace pisa
