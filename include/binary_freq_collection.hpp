#pragma once

#include <stdexcept>
#include <iterator>
#include <stdint.h>

#include "binary_collection.hpp"

namespace ds2i {

    class binary_freq_collection {
    public:

        binary_freq_collection(const char* basename)
            : m_docs((std::string(basename) + ".docs").c_str())
            , m_freqs((std::string(basename) + ".freqs").c_str())
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

        iterator end() const
        {
            return iterator(m_docs.end(), m_freqs.end());
        }

        uint64_t num_docs() const
        {
            return m_num_docs;
        }

        struct sequence {
            binary_collection::sequence docs;
            binary_collection::sequence freqs;
        };

        class iterator : public std::iterator<std::forward_iterator_tag,
                                              sequence> {
        public:
            iterator()
            {}

            value_type const& operator*() const
            {
                return m_cur_seq;
            }

            value_type const* operator->() const
            {
                return &m_cur_seq;
            }

            iterator& operator++()
            {
                m_cur_seq.docs = *++m_docs_it;
                m_cur_seq.freqs = *++m_freqs_it;
                return *this;
            }

            bool operator==(iterator const& other) const
            {
                return m_docs_it == other.m_docs_it;
            }

            bool operator!=(iterator const& other) const
            {
                return !(*this == other);
            }

        private:
            friend class binary_freq_collection;

            iterator(binary_collection::iterator docs_it,
                     binary_collection::iterator freqs_it)
                : m_docs_it(docs_it)
                , m_freqs_it(freqs_it)
            {
                m_cur_seq.docs = *m_docs_it;
                m_cur_seq.freqs = *m_freqs_it;
            }

            binary_collection::iterator m_docs_it;
            binary_collection::iterator m_freqs_it;
            sequence m_cur_seq;
        };

    private:
        binary_collection m_docs;
        binary_collection m_freqs;
        uint64_t m_num_docs;
    };
}
