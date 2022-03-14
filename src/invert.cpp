#include <numeric>
#include <sstream>
#include <vector>

#include <boost/filesystem.hpp>
#include <spdlog/spdlog.h>
#include <tbb/parallel_reduce.h>

#include "pisa/algorithm.hpp"
#include "pisa/binary_collection.hpp"
#include "pisa/invert.hpp"
#include "pisa/memory_source.hpp"
#include "pisa/payload_vector.hpp"
#include "pisa/util/inverted_index_utils.hpp"

template <typename T>
std::vector<T> concatenate(std::vector<std::vector<T>> const& containers)
{
    auto full_size = std::accumulate(
        containers.begin(), containers.end(), 0, [](auto const& acc, auto const& container) {
            return acc + container.size();
        });
    std::vector<T> vec(full_size);
    auto next_begin = std::begin(vec);
    for (auto const& container: containers) {
        std::copy(container.begin(), container.end(), next_begin);
        next_begin = std::next(next_begin, container.size());
    }
    return vec;
}

template <typename T>
std::istream& read_sequence(std::istream& is, std::vector<T>& out)
{
    uint32_t length;
    is.read(reinterpret_cast<char*>(&length), sizeof(length));
    auto size = out.size();
    out.resize(size + length);
    is.read(reinterpret_cast<char*>(&out[size]), length * sizeof(T));
    return is;
}

namespace pisa { namespace invert {

    auto map_to_postings(ForwardIndexSlice batch) -> std::vector<Posting>
    {
        auto docid = batch.document_ids.begin();
        std::vector<std::pair<Term_Id, Document_Id>> postings;
        for (auto const& document: batch.documents) {
            for (auto const& term: document) {
                postings.emplace_back(term, *docid);
            }
            ++docid;
        }
        return postings;
    }

    void join_term(
        std::vector<Document_Id>& lower_doc,
        std::vector<Frequency>& lower_freq,
        std::vector<Document_Id>& higher_doc,
        std::vector<Frequency>& higher_freq)
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

    auto invert_range(DocumentRange documents, Document_Id first_document_id, size_t threads)
        -> Inverted_Index
    {
        std::vector<uint32_t> document_sizes(documents.size());
        pisa::transform(
            pisa::execution::par_unseq,
            documents.begin(),
            documents.end(),
            document_sizes.begin(),
            [](auto const& terms) { return terms.size(); });
        gsl::index batch_size = (documents.size() + threads - 1) / threads;
        std::vector<ForwardIndexSlice> batches;
        for (gsl::index first_idx_in_batch = 0; first_idx_in_batch < documents.size();
             first_idx_in_batch += batch_size) {
            auto last_idx_in_batch = std::min(first_idx_in_batch + batch_size, documents.size());
            auto first_document_in_batch = first_document_id + first_idx_in_batch;
            auto last_document_in_batch = first_document_id + last_idx_in_batch;
            auto current_batch_size = last_idx_in_batch - first_idx_in_batch;
            batches.push_back(ForwardIndexSlice{
                documents.subspan(first_idx_in_batch, current_batch_size),
                ranges::views::iota(first_document_in_batch, last_document_in_batch)});
        }
        std::vector<std::vector<std::pair<Term_Id, Document_Id>>> posting_vectors(batches.size());

        pisa::transform(
            pisa::execution::par_unseq,
            batches.begin(),
            batches.end(),
            std::begin(posting_vectors),
            map_to_postings);

        auto postings = concatenate(posting_vectors);
        posting_vectors.clear();
        posting_vectors.shrink_to_fit();

        pisa::sort(pisa::execution::par_unseq, postings.begin(), postings.end());

        Inverted_Index index;
        tbb::parallel_reduce(tbb::blocked_range(postings.begin(), postings.end()), index);
        index.document_sizes = std::move(document_sizes);
        return index;
    }

    void
    write(std::string const& basename, invert::Inverted_Index const& index, std::uint32_t term_count)
    {
        std::ofstream dstream(basename + ".docs");
        std::ofstream fstream(basename + ".freqs");
        std::ofstream sstream(basename + ".sizes");
        std::uint32_t count = index.document_sizes.size();
        write_sequence(dstream, gsl::make_span<uint32_t const>(&count, 1));
        for (auto term: ranges::views::iota(Term_Id(0), Term_Id(term_count))) {
            if (auto pos = index.documents.find(term); pos != index.documents.end()) {
                auto const& documents = pos->second;
                auto const& frequencies = index.frequencies.at(term);
                write_sequence(dstream, gsl::span<Document_Id const>(documents));
                write_sequence(fstream, gsl::span<Frequency const>(frequencies));
            } else {
                write_sequence(dstream, gsl::span<Document_Id const>());
                write_sequence(fstream, gsl::span<Frequency const>());
            }
        }
        write_sequence(sstream, gsl::span<uint32_t const>(index.document_sizes));
    }

    [[nodiscard]] auto build_batches(
        std::string const& input_basename,
        std::string const& output_basename,
        InvertParams const& params) -> uint32_t
    {
        uint32_t batch = 0;
        binary_collection coll(input_basename.c_str());
        auto doc_iter = ++coll.begin();
        uint32_t documents_processed = 0;
        while (doc_iter != coll.end()) {
            std::vector<gsl::span<Term_Id const>> documents;
            for (; doc_iter != coll.end() && documents.size() < params.batch_size; ++doc_iter) {
                auto document_sequence = *doc_iter;
                documents.emplace_back(
                    reinterpret_cast<Term_Id const*>(document_sequence.begin()),
                    document_sequence.size());
            }
            spdlog::info(
                "Inverting [{}, {})", documents_processed, documents_processed + documents.size());
            auto index =
                invert_range(documents, Document_Id(documents_processed), params.num_threads);
            write(fmt::format("{}.batch.{}", output_basename, batch), index, *params.term_count);
            documents_processed += documents.size();
            batch += 1;
        }
        return batch;
    }

    void merge_batches(std::string const& output_basename, uint32_t batch_count, uint32_t term_count)
    {
        std::vector<binary_collection> doc_collections;
        std::vector<binary_collection> freq_collections;
        std::vector<uint32_t> document_sizes;
        for (auto batch: ranges::views::iota(uint32_t(0), batch_count)) {
            std::ostringstream batch_name_stream;
            batch_name_stream << output_basename << ".batch." << batch;
            doc_collections.emplace_back((batch_name_stream.str() + ".docs").c_str());
            freq_collections.emplace_back((batch_name_stream.str() + ".freqs").c_str());
            std::ifstream sizes_is(batch_name_stream.str() + ".sizes");
            read_sequence(sizes_is, document_sizes);
        }

        std::ofstream sos(output_basename + ".sizes");
        write_sequence(sos, gsl::span<uint32_t const>(document_sizes));

        std::vector<binary_collection::const_iterator> doc_iterators;
        std::vector<binary_collection::const_iterator> freq_iterators;
        std::transform(
            doc_collections.begin(),
            doc_collections.end(),
            std::back_inserter(doc_iterators),
            [](auto const& coll) { return ++coll.begin(); });
        std::transform(
            freq_collections.begin(),
            freq_collections.end(),
            std::back_inserter(freq_iterators),
            [](auto const& coll) { return coll.begin(); });

        std::ofstream dos(output_basename + ".docs");
        std::ofstream fos(output_basename + ".freqs");
        auto document_count = static_cast<uint32_t>(document_sizes.size());
        write_sequence(dos, gsl::make_span<uint32_t const>(&document_count, 1));
        size_t postings_count = 0;
        for (auto term_id: ranges::views::iota(uint32_t(0), term_count)) {
            std::vector<uint32_t> dlist;
            for (auto& iter: doc_iterators) {
                auto seq = *iter;
                dlist.insert(dlist.end(), seq.begin(), seq.end());
                ++iter;
            }
            std::vector<uint32_t> flist;
            for (auto& iter: freq_iterators) {
                auto seq = *iter;
                flist.insert(flist.end(), seq.begin(), seq.end());
                ++iter;
            }
            if (dlist.size() != flist.size()) {
                auto msg = fmt::format(
                    "Document and frequency lists must be equal length"
                    "but are {} and {} (term {})",
                    dlist.size(),
                    flist.size(),
                    term_id);
                spdlog::error(msg);
                throw std::runtime_error(msg);
            }
            if (dlist.empty()) {
                auto msg = fmt::format("Posting list must be non-empty (term {})", term_id);
                spdlog::error(msg);
                throw std::runtime_error(msg);
            }
            postings_count += dlist.size();
            write_sequence(dos, gsl::span<uint32_t const>(dlist));
            write_sequence(fos, gsl::span<uint32_t const>(flist));
        }

        spdlog::info("Number of terms: {}", term_count);
        spdlog::info("Number of documents: {}", document_count);
        spdlog::info("Number of postings: {}", postings_count);
    }

    void invert_forward_index(
        std::string const& input_basename, std::string const& output_basename, InvertParams params)
    {
        if (not params.term_count) {
            auto source = MemorySource::mapped_file(fmt::format("{}.termlex", input_basename));
            auto terms = Payload_Vector<>::from(source);
            params.term_count = static_cast<std::uint32_t>(terms.size());
        }

        uint32_t batch_count = invert::build_batches(input_basename, output_basename, params);
        invert::merge_batches(output_basename, batch_count, *params.term_count);

        for (auto batch: ranges::views::iota(uint32_t(0), batch_count)) {
            std::ostringstream batch_name_stream;
            batch_name_stream << output_basename << ".batch." << batch;
            auto batch_basename = batch_name_stream.str();
            boost::filesystem::remove(boost::filesystem::path{batch_basename + ".docs"});
            boost::filesystem::remove(boost::filesystem::path{batch_basename + ".freqs"});
            boost::filesystem::remove(boost::filesystem::path{batch_basename + ".sizes"});
        }
    }

    Inverted_Index::Inverted_Index(Inverted_Index&, tbb::split) {}
    Inverted_Index::Inverted_Index(
        Documents documents, Frequencies frequencies, std::vector<std::uint32_t> document_sizes)
        : documents(std::move(documents)),
          frequencies(std::move(frequencies)),
          document_sizes(std::move(document_sizes))
    {}

    void Inverted_Index::operator()(tbb::blocked_range<PostingIterator> const& r)
    {
        if (auto first = r.begin(); first != r.end()) {
            if (auto current_term = first->first; not documents[current_term].empty()) {
                auto current_doc = documents[current_term].back();
                auto& freq = frequencies[current_term].back();
                auto last = std::find_if(first, r.end(), [&](auto const& posting) {
                    return posting.first != current_term || posting.second != current_doc;
                });
                freq += Frequency(std::distance(first, last));
                first = last;
            }
            while (first != r.end()) {
                auto [current_term, current_doc] = *first;
                auto last = std::find_if(
                    first,
                    r.end(),
                    [&, current_term = current_term, current_doc = current_doc](auto const& posting) {
                        return posting.first != current_term || posting.second != current_doc;
                    });
                auto freq = Frequency(std::distance(first, last));
                documents[current_term].push_back(current_doc);
                frequencies[current_term].push_back(freq);
                first = last;
            }
        }
    }

    void Inverted_Index::join(Inverted_Index& rhs)
    {
        document_sizes.insert(
            document_sizes.end(), rhs.document_sizes.begin(), rhs.document_sizes.end());
        for (auto&& [term_id, document_ids]: rhs.documents) {
            if (auto pos = documents.find(term_id); pos == documents.end()) {
                std::swap(documents[term_id], document_ids);
                std::swap(frequencies[term_id], rhs.frequencies[term_id]);
            } else if (pos->second.back() <= document_ids.front()) {
                join_term(pos->second, frequencies[term_id], document_ids, rhs.frequencies[term_id]);
            } else {
                join_term(document_ids, rhs.frequencies[term_id], pos->second, frequencies[term_id]);
                std::swap(documents[term_id], document_ids);
                std::swap(frequencies[term_id], rhs.frequencies[term_id]);
            }
        }
    }
}}  // namespace pisa::invert
