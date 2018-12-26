#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "binary_collection.hpp"
#include "codec/block_codecs.hpp"
#include "codec/varintgb.hpp"
#include "util/progress.hpp"

namespace ds2i {

using id_type = std::uint32_t;

//! This class represents a forward index.
//!
//! The documents IDs are assumed to be consecutive numbers [0, N), where N is the collection size.
//! Each entry contains an encoded list of terms for the given document.
class forward_index : public std::vector<std::vector<std::uint8_t>> {
   public:
    using id_type    = uint32_t;
    using entry_type = std::vector<std::uint8_t>;

    //! Initializes a new forward index with empty containers.
    forward_index(size_t document_count, size_t term_count, bool compressed = true)
        : std::vector<entry_type>(document_count),
          m_term_count(term_count),
          m_term_counts(document_count),
          m_terms(term_count),
          m_compressed(compressed) {}

    const std::size_t &term_count() const { return m_term_count; }
    const std::size_t &term_count(id_type document) const { return m_term_counts[document]; }

    //! Compresses each document in `fwd` with a faster codec.
    static forward_index read(const std::string &input_file) {
        std::ifstream in(input_file.c_str());
        bool          compressed;
        size_t        term_count, docs_count;
        in.read(reinterpret_cast<char *>(&compressed), sizeof(compressed));
        in.read(reinterpret_cast<char *>(&term_count), sizeof(term_count));
        in.read(reinterpret_cast<char *>(&docs_count), sizeof(docs_count));
        forward_index fwd(docs_count, term_count, compressed);
        for (id_type doc = 0; doc < docs_count; ++doc) {
            size_t block_size;
            in.read(reinterpret_cast<char *>(&term_count), sizeof(term_count));
            in.read(reinterpret_cast<char *>(&block_size), sizeof(block_size));
            fwd.m_term_counts[doc] = term_count;
            fwd[doc].resize(block_size);
            in.read(reinterpret_cast<char *>(fwd[doc].data()), block_size);
        }
        return fwd;
    }

    static forward_index &compress(forward_index &fwd) {
        progress p("Compressing forward index", fwd.size());
        for (id_type doc = 0u; doc < fwd.size(); ++doc) {
            auto &               encoded_terms = fwd[doc];
            std::vector<id_type> terms(encoded_terms.size() * 5);
            std::size_t          n = 0;
            TightVariableByte::decode(encoded_terms.data(), terms.data(), encoded_terms.size(), n);
            fwd.m_term_counts[doc] = n;
            encoded_terms.clear();
            encoded_terms.resize(2 * n * sizeof(id_type));
            VarIntGB<false> varintgb_codec;
            std::size_t     byte_size =
                varintgb_codec.encodeArray(terms.data(), n, encoded_terms.data());
            encoded_terms.resize(byte_size);
            encoded_terms.shrink_to_fit();
            p.update(1);
        }
        return fwd;
    }

    static forward_index from_inverted_index(const std::string &input_basename,
                                             size_t             min_len,
                                             bool               use_compression) {
        binary_collection coll((input_basename + ".docs").c_str());

        auto firstseq = *coll.begin();
        if (firstseq.size() != 1) {
            throw std::invalid_argument("First sequence should only contain number of documents");
        }
        auto num_docs  = *firstseq.begin();
        auto num_terms = std::distance(++coll.begin(), coll.end());

        forward_index fwd(num_docs, num_terms, use_compression);
        {
            progress             p("Building forward index", num_terms);
            id_type              tid = 0;
            std::vector<id_type> prev(num_docs, 0u);
            for (auto it = ++coll.begin(); it != coll.end(); ++it) {
                for (const auto &d : *it) {
                    if (it->size() >= min_len) {
                        TightVariableByte::encode_single(tid - prev[d], fwd[d]);
                        prev[d] = tid;
                        fwd.m_term_counts[d]++;
                    }
                }
                p.update(1);
                ++tid;
            }
        }
        if (use_compression) {
            compress(fwd);
        }

        return fwd;
    }

    static void write(const forward_index &fwd, const std::string &output_file) {
        std::ofstream out(output_file.c_str());
        size_t        size = fwd.size();
        out.write(reinterpret_cast<const char *>(&fwd.m_compressed), sizeof(fwd.m_compressed));
        out.write(reinterpret_cast<const char *>(&fwd.m_term_count), sizeof(fwd.m_term_count));
        out.write(reinterpret_cast<const char *>(&size), sizeof(size));
        for (id_type doc = 0; doc < fwd.size(); ++doc) {
            size = fwd[doc].size();
            out.write(reinterpret_cast<const char *>(&fwd.m_term_counts[doc]),
                      sizeof(fwd.m_term_counts[doc]));
            out.write(reinterpret_cast<const char *>(&size), sizeof(size));
            out.write(reinterpret_cast<const char *>(fwd[doc].data()), size);
        }
    }

    //! Decodes and returns the list of terms for a given document.
    std::vector<id_type> terms(id_type document) const {
        const entry_type &    encoded_terms = (*this)[document];
        std::vector<uint32_t> terms;
        if (m_compressed) {
            std::size_t term_count = m_term_counts[document];
            terms.resize(term_count);
            VarIntGB<true> varintgb_codec;
            varintgb_codec.decodeArray(encoded_terms.data(), term_count, terms.data());
        } else {
            terms.resize(encoded_terms.size() * 5);
            size_t n = 0;
            TightVariableByte::decode(encoded_terms.data(), terms.data(), encoded_terms.size(), n);
            terms.resize(n);
            terms.shrink_to_fit();
        }
        return terms;
    }

   private:
    std::size_t              m_term_count;
    std::vector<std::size_t> m_term_counts;
    std::size_t              m_terms;
    bool                     m_compressed;
};

} // namespace ds2i
