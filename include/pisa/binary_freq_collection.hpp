#pragma once

#include <cstdint>
#include <iterator>
#include <stdexcept>

#include "binary_collection.hpp"

namespace pisa {

class binary_freq_collection {
  public:
    explicit binary_freq_collection(const char* basename)
        : m_docs((std::string(basename) + ".docs").c_str()),
          m_freqs((std::string(basename) + ".freqs").c_str())
    {
        auto firstseq = *m_docs.begin();
        if (firstseq.size() != 1) {
            throw std::invalid_argument("First sequence should only contain number of documents");
        }
        m_num_docs = *firstseq.begin();
    }

    class iterator;

    iterator begin() const
    {
        auto docs_it = m_docs.begin();
        return iterator(++docs_it, m_freqs.begin());
    }

    iterator end() const { return iterator(m_docs.end(), m_freqs.end()); }

    size_t size() const { return std::distance(begin(), end()); }

    uint64_t num_docs() const { return m_num_docs; }

    struct sequence {
        binary_collection::const_sequence docs;
        binary_collection::const_sequence freqs;
    };

    class iterator {
      public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = sequence;
        using difference_type = std::ptrdiff_t;
        using pointer = value_type*;
        using reference = value_type&;

        iterator() = default;

        sequence const& operator*() const { return m_cur_seq; }

        sequence const* operator->() const { return &m_cur_seq; }

        iterator& operator++()
        {
            m_cur_seq.docs = *++m_docs_it;
            m_cur_seq.freqs = *++m_freqs_it;
            return *this;
        }

        bool operator==(iterator const& other) const { return m_docs_it == other.m_docs_it; }

        bool operator!=(iterator const& other) const { return !(*this == other); }

      private:
        friend class binary_freq_collection;

        iterator(binary_collection::const_iterator docs_it, binary_collection::const_iterator freqs_it)
            : m_docs_it(docs_it), m_freqs_it(freqs_it)
        {
            m_cur_seq.docs = *m_docs_it;
            m_cur_seq.freqs = *m_freqs_it;
        }

        binary_collection::const_iterator m_docs_it;
        binary_collection::const_iterator m_freqs_it;
        sequence m_cur_seq;
    };

  private:
    binary_collection m_docs;
    binary_collection m_freqs;
    uint64_t m_num_docs;
};
}  // namespace pisa
