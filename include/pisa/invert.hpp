#pragma once

#include <optional>
#include <thread>
#include <vector>

#include <gsl/span>
#include <range/v3/view/iota.hpp>
#include <tbb/blocked_range.h>

#include "pisa/type_safe.hpp"

namespace pisa { namespace invert {

    using Posting = std::pair<Term_Id, Document_Id>;
    using PostingIterator = typename std::vector<Posting>::iterator;
    using Documents = std::unordered_map<Term_Id, std::vector<Document_Id>>;
    using Frequencies = std::unordered_map<Term_Id, std::vector<Frequency>>;
    using DocumentRange = gsl::span<gsl::span<Term_Id const>>;

    /// Inverted index abstraction used internally in the inverting process.
    ///
    /// This is only meant to store an index for a limited range of documents.
    /// These batches are written to disk and then merged at a later stage.
    struct Inverted_Index {
        /// Maps a term to its list of documents.
        Documents documents{};
        /// Maps a term to its list of frequencies. This is aligned with `documents`.
        Frequencies frequencies{};
        /// List of document sizes for all documents in the range.
        std::vector<std::uint32_t> document_sizes{};

        Inverted_Index() = default;
        Inverted_Index(Inverted_Index&, tbb::split);
        Inverted_Index(
            Documents documents,
            Frequencies frequencies,
            std::vector<std::uint32_t> document_sizes = {});

        void operator()(tbb::blocked_range<PostingIterator> const& r);
        void join(Inverted_Index& rhs);
    };

    /// A single slice view over a chunk of a forward index.
    struct ForwardIndexSlice {
        gsl::span<gsl::span<Term_Id const>> documents;
        ranges::iota_view<Document_Id, Document_Id> document_ids;
    };

    /// Maps a forward index slice to a vector of postings.
    auto map_to_postings(ForwardIndexSlice batch) -> std::vector<Posting>;

    /// Joins postings in the inverted index for a single term.
    /// The first two arguments are the postings with lower document IDs.
    /// They could potentially overlap, such that the last document in the
    /// first list is the first document in the latter.
    void join_term(
        std::vector<Document_Id>& lower_doc,
        std::vector<Frequency>& lower_freq,
        std::vector<Document_Id>& higher_doc,
        std::vector<Frequency>& higher_freq);

    /// Creates an in-memory inverted index for a single document range.
    auto invert_range(DocumentRange documents, Document_Id first_document_id, size_t threads)
        -> Inverted_Index;

    /// Parameters for inverting process.
    struct InvertParams {
        std::size_t batch_size = 100'000;
        std::size_t num_threads = std::thread::hardware_concurrency() + 1;
        std::optional<std::uint32_t> term_count = std::nullopt;
    };

    /// Creates an inverted index (simple, uncompressed binary format) from a forward index.
    void invert_forward_index(
        std::string const& input_basename, std::string const& output_basename, InvertParams params);

}}  // namespace pisa::invert
