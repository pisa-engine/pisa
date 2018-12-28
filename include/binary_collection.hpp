#pragma once

#include <stdexcept>
#include <iterator>
#include <cstdint>
#include "mio/mmap.hpp"
#include "util/util.hpp"

#if defined (__unix__) || (defined (__APPLE__) && defined (__MACH__))
#include <sys/mman.h>
#endif

namespace ds2i {

    class binary_collection {
    public:
        typedef uint32_t posting_type;

        binary_collection(const char* filename)
        {
            std::error_code error;
            m_file.map(filename, error);
            if ( error ) {
                std::cerr << "error mapping file: " << error.message() << ", exiting..." << std::endl;
                throw std::runtime_error("Error opening file");
            }
            m_data = (posting_type const*)m_file.data();
            m_data_size = m_file.size() / sizeof(m_data[0]);

#if defined (__unix__) || (defined (__APPLE__) && defined (__MACH__))
            // Indicates that the application expects to access this address range in a sequential manner
            auto ret = posix_madvise((void*)m_data, m_data_size, POSIX_MADV_SEQUENTIAL);
            if (ret) logger() << "Error calling madvice: " << errno << std::endl;
#endif
        }

        class iterator;

        iterator begin() const
        {
            return iterator(this, 0);
        }

        iterator end() const
        {
            return iterator(this, m_data_size);
        }

        class sequence {
        public:
            sequence()
                : m_begin(nullptr)
                , m_end(nullptr)
            {}

            posting_type const* begin() const
            {
                return m_begin;
            }

            posting_type const* end() const
            {
                return m_end;
            }

            posting_type back() const
            {
                assert(size());
                return *(m_end - 1);
            }

            size_t size() const
            {
                return m_end - m_begin;
            }

        private:
            friend class binary_collection::iterator;

            sequence(posting_type const* begin, posting_type const* end)
                : m_begin(begin)
                , m_end(end)
            {}

            posting_type const* m_begin;
            posting_type const* m_end;
        };

        class iterator : public std::iterator<std::forward_iterator_tag,
                                               sequence> {
        public:
            iterator()
                : m_collection(nullptr)
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
                m_pos = m_next_pos;
                read();
                return *this;
            }

            bool operator==(iterator const& other) const
            {
                assert(m_collection == other.m_collection);
                return m_pos == other.m_pos;
            }

            bool operator!=(iterator const& other) const
            {
                return !(*this == other);
            }

        private:
            friend class binary_collection;

            iterator(binary_collection const* coll, size_t pos)
                : m_collection(coll)
                , m_pos(pos)
            {
                read();
            }

            void read()
            {
                assert(m_pos <= m_collection->m_data_size);
                if (m_pos == m_collection->m_data_size) return;

                size_t n = 0;
                size_t pos = m_pos;
                while (!(n = m_collection->m_data[pos++])); // skip empty seqs
                // file might be truncated
                n = std::min(n, size_t(m_collection->m_data_size - pos));
                posting_type const* begin = &m_collection->m_data[pos];
                posting_type const* end = begin + n;

                m_next_pos = pos + n;
                m_cur_seq = sequence(begin, end);
            }

            binary_collection const* m_collection;
            size_t m_pos, m_next_pos;
            sequence m_cur_seq;
        };

    private:
        mio::mmap_source m_file;
        posting_type const* m_data;
        size_t m_data_size;
    };
}
