#include "binary_collection.hpp"

#include <algorithm>
#include <stdexcept>

#include <mio/mmap.hpp>
#include <spdlog/spdlog.h>

#if defined(__unix__) || (defined(__APPLE__) && defined(__MACH__))
#include <sys/mman.h>
#endif

namespace pisa {

BinaryCollection::BinaryCollection(char const *filename)
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
    // Indicates that the application expects to access this address range in a sequential manner
    auto ret = posix_madvise((void *)m_data, m_data_size, POSIX_MADV_SEQUENTIAL);
    if (ret) {
        spdlog::error("Error calling madvice: {}", errno);
    }
#endif
}

BinaryCollection::BinaryCollection(std::string const &filename) : BinaryCollection(filename.c_str())
{
}

[[nodiscard]] auto BinaryCollection::begin() -> iterator { return iterator(this, 0); }
[[nodiscard]] auto BinaryCollection::end() -> iterator { return iterator(this, m_data_size); }
[[nodiscard]] auto BinaryCollection::begin() const -> const_iterator
{
    return const_iterator(this, 0);
}
[[nodiscard]] auto BinaryCollection::end() const -> const_iterator
{
    return const_iterator(this, m_data_size);
}
[[nodiscard]] auto BinaryCollection::cbegin() const -> const_iterator
{
    return const_iterator(this, 0);
}
[[nodiscard]] auto BinaryCollection::cend() const -> const_iterator
{
    return const_iterator(this, m_data_size);
}

namespace detail {

    template <typename Sequence, typename Iterator>
    void read_next_sequence(Iterator &iter)
    {
        assert(iter.m_pos <= iter.m_data_size);
        if (iter.m_pos == iter.m_data_size) {
            return;
        }
        std::size_t pos = iter.m_pos;
        // file might be truncated
        std::size_t n = iter.m_data[pos++];
        n = std::min(n, iter.m_data_size - pos);
        auto begin = &iter.m_data[pos];
        iter.m_next_pos = pos + n;
        iter.m_current_seqence = Sequence(begin, begin + n);
    }

} // namespace detail

BinaryCollection::iterator::iterator(BinaryCollection const *collection, size_type position)
    : m_data(collection->m_data), m_data_size(collection->m_data_size), m_pos(position)
{
    detail::read_next_sequence<sequence, iterator>(*this);
}

auto BinaryCollection::iterator::operator++() -> iterator &
{
    m_pos = m_next_pos;
    detail::read_next_sequence<sequence, iterator>(*this);
    return *this;
}

BinaryCollection::const_iterator::const_iterator(BinaryCollection const *collection,
                                                 size_type position)
    : m_data(collection->m_data), m_data_size(collection->m_data_size), m_pos(position)
{
    detail::read_next_sequence<const_sequence, const_iterator>(*this);
}

auto BinaryCollection::const_iterator::operator++() -> const_iterator &
{
    m_pos = m_next_pos;
    detail::read_next_sequence<const_sequence, const_iterator>(*this);
    return *this;
}

} // namespace pisa
