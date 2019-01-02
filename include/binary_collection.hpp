#pragma once

#include <stdexcept>
#include <iterator>
#include <cstdint>

#include "mio/mmap.hpp"
#include "gsl/span"

#include "util/util.hpp"

#if defined (__unix__) || (defined (__APPLE__) && defined (__MACH__))
#include <sys/mman.h>
#endif

namespace ds2i {

    class binary_collection {
       public:
        using posting_type = uint32_t;

        binary_collection(const char *filename) {
            std::error_code error;
            m_file.map(filename, error);
            if ( error ) {
                std::cerr << "error mapping file: " << error.message() << ", exiting..." << std::endl;
                throw std::runtime_error("Error opening file");
            }
            m_data      = reinterpret_cast<posting_type *>(m_file.data());
            m_data_size = m_file.size() / sizeof(m_data[0]);

#if defined (__unix__) || (defined (__APPLE__) && defined (__MACH__))
            // Indicates that the application expects to access this address range in a sequential manner
            auto ret = posix_madvise((void*)m_data, m_data_size, POSIX_MADV_SEQUENTIAL);
            if (ret) logger() << "Error calling madvice: " << errno << std::endl;
#endif
        }

        template <typename T>
        class base_iterator;

        using iterator       = base_iterator<posting_type>;
        using const_iterator = base_iterator<posting_type const>;

        iterator       begin() { return iterator(this, 0); }
        iterator       end() { return iterator(this, m_data_size); }
        const_iterator begin() const { return const_iterator(this, 0); }
        const_iterator end() const { return const_iterator(this, m_data_size); }
        const_iterator cbegin() const { return const_iterator(this, 0); }
        const_iterator cend() const { return const_iterator(this, m_data_size); }

        using sequence       = gsl::span<posting_type>;
        using const_sequence = gsl::span<posting_type const>;

        template <typename T>
        class base_iterator : public std::iterator<std::forward_iterator_tag, gsl::span<T>> {
           public:
            base_iterator() : m_collection(nullptr) {}

            auto const &operator*() const { return m_cur_seq; }

            auto const *operator-> () const { return &m_cur_seq; }

            base_iterator &operator++() {
                m_pos = m_next_pos;
                read();
                return *this;
            }

            bool operator==(base_iterator const &other) const {
                assert(m_collection == other.m_collection);
                return m_pos == other.m_pos;
            }

            bool operator!=(base_iterator const &other) const { return !(*this == other); }

           private:
            friend class binary_collection;

            base_iterator(binary_collection const *coll, size_t pos)
                : m_collection(coll), m_pos(pos) {
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
                T *begin = &m_collection->m_data[pos];

                m_next_pos = pos + n;
                m_cur_seq  = gsl::span<T>(begin, n);
            }

            binary_collection const *     m_collection;
            size_t                        m_pos, m_next_pos;
            gsl::span<T>                  m_cur_seq;
        };

       private:
        mio::mmap_sink m_file;
        posting_type * m_data;
        size_t         m_data_size;
    };
}
