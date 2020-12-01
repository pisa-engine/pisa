#include "memory_source.hpp"

#include <exception>

#include "io.hpp"

namespace pisa {

constexpr std::string_view EMPTY_MEMORY = "Empty memory source";

auto MemorySource::from_vector(std::vector<char> vec) -> MemorySource
{
    return MemorySource(std::move(vec));
}

auto MemorySource::from_span(gsl::span<char> span) -> MemorySource
{
    return MemorySource(span);
}

auto MemorySource::mapped_file(std::string const& file) -> MemorySource
{
    return MemorySource::mapped_file(io::resolve_path(file));
}

auto MemorySource::mapped_file(boost::filesystem::path file) -> MemorySource
{
    if (not boost::filesystem::exists(file)) {
        throw io::NoSuchFile(file.string());
    }
    return MemorySource(mio::mmap_source(file.string().c_str()));
}

auto MemorySource::is_mapped() noexcept -> bool
{
    return m_source != nullptr;
}

auto MemorySource::data() const -> pointer
{
    if (m_source == nullptr) {
        throw std::domain_error(std::string(EMPTY_MEMORY));
    }
    return m_source->data();
}

auto MemorySource::begin() const -> pointer
{
    if (m_source == nullptr) {
        throw std::domain_error(std::string(EMPTY_MEMORY));
    }
    return m_source->data();
}

auto MemorySource::end() const -> pointer
{
    if (m_source == nullptr) {
        throw std::domain_error(std::string(EMPTY_MEMORY));
    }
    return std::next(m_source->data(), m_source->size());
}

auto MemorySource::size() const -> size_type
{
    if (m_source == nullptr) {
        return 0;
    }
    return m_source->size();
}

auto MemorySource::span() const -> gsl::span<value_type const>
{
    if (m_source == nullptr) {
        return gsl::span<value_type const>();
    }
    return gsl::span<value_type const>(begin(), size());
}

auto MemorySource::subspan(size_type offset, size_type size) const -> gsl::span<value_type const>
{
    if (m_source == nullptr) {
        if (offset == 0 && (size == 0 || size == gsl::dynamic_extent)) {
            return gsl::span<value_type const>();
        }
        throw std::out_of_range("Subspan out of bounds");
    }
    if (offset > this->size() || offset + size > this->size()) {
        throw std::out_of_range("Subspan out of bounds");
    }
    return span().subspan(offset, size);
}

}  // namespace pisa
