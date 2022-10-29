#pragma once

#include <cstdint>
#include <iterator>
#include <stdexcept>
#include <type_traits>

#include "fmt/format.h"
#include "mio/mmap.hpp"
#include "spdlog/spdlog.h"

#include "util/util.hpp"

#if defined(__unix__) || (defined(__APPLE__) && defined(__MACH__))
    #include <sys/mman.h>
#endif

namespace pisa {

template <typename Source = mio::mmap_source>
class base_binary_collection {
  public:
    using posting_type = uint32_t;
    using pointer = typename std::
        conditional<std::is_same<Source, mio::mmap_source>::value, posting_type const, posting_type>::type*;

    explicit base_binary_collection(const char* filename)
    {
        std::error_code error;
        m_file.map(filename, error);
        if (error) {
            spdlog::error("Error mapping file {}: {}", filename, error.message());
            throw std::runtime_error("Error opening file");
        }
        m_data = reinterpret_cast<pointer>(m_file.data());
        m_data_size = m_file.size() / sizeof(m_data[0]);

#if defined(__unix__) || (defined(__APPLE__) && defined(__MACH__))
        // Indicates that the application expects to access this address range in a sequential
        // manner
        auto ret = posix_madvise((void*)m_data, m_data_size, POSIX_MADV_SEQUENTIAL);
        if (ret != 0) {
            spdlog::error("Error calling madvice: {}", errno);
        }
#endif
    }

    class sequence {
      public:
        sequence(pointer begin, pointer end) : m_begin(begin), m_end(end) {}
        sequence() : m_begin(nullptr), m_end(nullptr) {}

        posting_type const& operator[](size_t p) const { return *(m_begin + p); }

        pointer begin() const { return m_begin; }
        pointer end() const { return m_end; }
        size_t size() const { return m_end - m_begin; }

        posting_type back() const
        {
            assert(size());
            return *(m_end - 1);
        }

      private:
        pointer m_begin;
        pointer m_end;
    };

    using const_sequence = sequence;

    template <typename S>
    class base_iterator;

    using const_iterator = base_iterator<const_sequence>;
    using iterator = typename std::conditional<
        std::is_same<Source, mio::mmap_source>::value,
        const_iterator,
        base_iterator<sequence>>::type;

    iterator begin() { return iterator(this, 0); }
    iterator end() { return iterator(this, m_data_size); }
    const_iterator begin() const { return const_iterator(this, 0); }
    const_iterator end() const { return const_iterator(this, m_data_size); }
    const_iterator cbegin() const { return const_iterator(this, 0); }
    const_iterator cend() const { return const_iterator(this, m_data_size); }

    template <typename S>
    class base_iterator {
      public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = S;
        using difference_type = std::ptrdiff_t;
        using pointer = value_type*;
        using reference = value_type&;

        base_iterator() : m_data(nullptr) {}

        auto const& operator*() const { return m_cur_seq; }

        auto const* operator->() const { return &m_cur_seq; }

        base_iterator& operator++()
        {
            m_pos = m_next_pos;
            read();
            return *this;
        }

        bool operator==(base_iterator const& other) const
        {
            assert(m_data == other.m_data);
            assert(m_data_size == other.m_data_size);
            return m_pos == other.m_pos;
        }

        bool operator!=(base_iterator const& other) const { return !(*this == other); }

      private:
        friend class base_binary_collection;

        base_iterator(base_binary_collection const* coll, size_t pos)
            : m_data(coll->m_data), m_data_size(coll->m_data_size), m_pos(pos)
        {
            read();
        }

        void read()
        {
            assert(m_pos <= m_data_size);
            if (m_pos == m_data_size) {
                return;
            }

            size_t n = 0;
            size_t pos = m_pos;
            n = m_data[pos++];
            // file might be truncated
            n = std::min(n, size_t(m_data_size - pos));
            auto begin = &m_data[pos];

            m_next_pos = pos + n;
            m_cur_seq = S(begin, begin + n);
        }

        typename base_binary_collection::pointer const m_data;
        size_t m_data_size = 0, m_pos = 0, m_next_pos = 0;
        S m_cur_seq;
    };

  private:
    Source m_file;
    typename base_binary_collection::pointer m_data;
    size_t m_data_size;
};

using binary_collection = base_binary_collection<>;
using writable_binary_collection = base_binary_collection<mio::mmap_sink>;
}  // namespace pisa
