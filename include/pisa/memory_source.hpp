#pragma once

#include <memory>

#include <boost/filesystem/path.hpp>
#include <gsl/span>
#include <mio/mmap.hpp>

namespace pisa {

/// This is an owning memory source for any byte-based structures.
class MemorySource {
  public:
    using value_type = char;
    using pointer = value_type const*;
    using size_type = typename mio::mmap_source::size_type;

    MemorySource() = default;
    MemorySource(MemorySource const&) = delete;
    MemorySource(MemorySource&&) noexcept = default;
    MemorySource& operator=(MemorySource const&) = delete;
    MemorySource& operator=(MemorySource&&) noexcept = default;
    ~MemorySource() = default;

    /// Constructs a memory source using a memory mapped file.
    ///
    /// \throws NoSuchFile          if the file doesn't exist
    /// \throws std::system_error   if fails to map the file.
    [[nodiscard]] static auto mapped_file(std::string const& file) -> MemorySource;

    /// Constructs a memory source using a memory mapped file.
    ///
    /// \throws NoSuchFile          if the file doesn't exist
    /// \throws std::system_error   if fails to map the file.
    [[nodiscard]] static auto mapped_file(boost::filesystem::path file) -> MemorySource;

    /// Checks if memory is mapped.
    [[nodiscard]] auto is_mapped() noexcept -> bool;

    /// Pointer to the first byte.
    ///
    /// \throws std::domain_error   if memory is empty
    [[nodiscard]] auto data() const -> pointer;

    /// Pointer to the first byte.
    ///
    /// \throws std::domain_error   if memory is empty
    [[nodiscard]] auto begin() const -> pointer;

    /// Pointer to the address after the last byte.
    ///
    /// \throws std::domain_error   if memory is empty
    [[nodiscard]] auto end() const -> pointer;

    /// Number of bytes in the source.
    [[nodiscard]] auto size() const -> size_type;

    /// Full span over memory.
    [[nodiscard]] auto span() const -> gsl::span<value_type const>;

    /// Subspan of memory.
    ///
    /// \throws std::out_of_range   if offset + size is out of bounds
    [[nodiscard]] auto subspan(size_type offset, size_type size = gsl::dynamic_extent) const
        -> gsl::span<value_type const>;

  private:
    explicit MemorySource(std::unique_ptr<mio::mmap_source> source) : m_source(std::move(source)) {}

    std::unique_ptr<mio::mmap_source> m_source;
};

}  // namespace pisa
