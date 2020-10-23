#include "memory_source.hpp"

#include <exception>

#include "io.hpp"

namespace pisa {

constexpr std::string_view EMPTY_MEMORY = "Empty memory source";

template <typename Container>
struct ViewWrapper {
    using value_type = std::decay_t<typename Container::value_type>;
    static_assert(std::is_pod_v<value_type> && sizeof(value_type) == sizeof(char));

    Container container;
    [[nodiscard]] MemoryResult subspan(std::size_t offset, std::size_t size) const
    {
        return MemoryResult{
            gsl::span<char const>(reinterpret_cast<char const*>(container.data()), container.size())
                .subspan(offset, size),
            {}};
    }
    [[nodiscard]] std::size_t size() const { return container.size(); }
};

class LazyFileSource {
  public:
    LazyFileSource(std::ifstream in, std::size_t file_size)
        : m_in(std::move(in)), m_file_size(file_size)
    {}

    [[nodiscard]] MemoryResult subspan(std::size_t offset, std::size_t size) const
    {
        if (size == gsl::dynamic_extent) {
            size = this->size() - offset;
        }
        auto buffer = std::make_shared<std::vector<char>>(size);
        m_in.seekg(offset, std::ios_base::beg);
        if (not m_in.read(&(*buffer)[0], size)) {
            throw std::out_of_range("Out of bound read.");
        }
        return MemoryResult{gsl::span<char const>(buffer->data(), size), std::move(buffer)};
    }

    [[nodiscard]] std::size_t size() const { return m_file_size; }

  private:
    mutable std::ifstream m_in;
    std::size_t m_file_size;
};

auto MemorySource::from_vector(std::vector<char> vec) -> MemorySource
{
    return MemorySource(ViewWrapper<std::vector<char>>{std::move(vec)});
}

auto MemorySource::from_vector(std::vector<std::uint8_t> vec) -> MemorySource
{
    return MemorySource(ViewWrapper<std::vector<std::uint8_t>>{std::move(vec)});
}

auto MemorySource::from_span(gsl::span<char const> span) -> MemorySource
{
    return MemorySource(ViewWrapper<gsl::span<char const>>{span});
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
    return MemorySource(ViewWrapper<mio::mmap_source>{mio::mmap_source(file.string().c_str())});
}

auto MemorySource::disk_resident_file(std::string const& file) -> MemorySource
{
    return MemorySource::disk_resident_file(io::resolve_path(file));
}

auto MemorySource::disk_resident_file(boost::filesystem::path file) -> MemorySource
{
    if (not boost::filesystem::exists(file)) {
        throw io::NoSuchFile(file.string());
    }
    auto size = boost::filesystem::file_size(file);
    return MemorySource(LazyFileSource(std::ifstream(file.c_str()), size));
}

auto MemorySource::is_mapped() noexcept -> bool
{
    return m_source != nullptr;
}

auto MemorySource::size() const -> size_type
{
    if (m_source == nullptr) {
        return 0;
    }
    return m_source->size();
}

auto MemorySource::subspan(size_type offset, size_type size) const -> MemorySpan
{
    if (m_source == nullptr) {
        throw std::domain_error(std::string(EMPTY_MEMORY));
    }
    if (offset > this->size() || (size != gsl::dynamic_extent && offset + size > this->size())) {
        throw std::out_of_range("Subspan out of bounds");
    }
    auto res = m_source->subspan(offset, size);
    return MemorySpan(m_source, res.span, std::move(res.owned));
}

MemorySpan::MemorySpan(
    std::shared_ptr<typename MemorySource::Interface> source,
    gsl::span<value_type const> memory_span,
    std::shared_ptr<std::vector<value_type>> owning_memory)
    : m_source(std::move(source)),
      m_memory_span(memory_span),
      m_owning_memory(std::move(owning_memory))
{}

auto MemorySpan::data() const -> pointer
{
    return m_memory_span.data();
}

auto MemorySpan::begin() const -> pointer
{
    return m_memory_span.data();
}

auto MemorySpan::end() const -> pointer
{
    return std::next(m_memory_span.data(), size());
}

auto MemorySpan::size() const -> size_type
{
    return m_memory_span.size();
}

auto MemorySpan::span() const -> gsl::span<value_type const>
{
    return m_memory_span;
}
auto MemorySpan::subspan(size_type offset, size_type size) const -> gsl::span<value_type const>
{
    return m_memory_span.subspan(offset, size);
}

}  // namespace pisa
