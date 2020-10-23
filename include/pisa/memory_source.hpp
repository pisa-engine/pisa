#pragma once

#include <memory>

#include <boost/filesystem/path.hpp>
#include <gsl/span>
#include <mio/mmap.hpp>

namespace pisa {

class MemorySpan;

struct MemoryResult {
    gsl::span<char const> span;
    std::shared_ptr<std::vector<char>> owned;
};

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

    /// Constructs a memory source from a vector.
    [[nodiscard]] static auto from_vector(std::vector<char> vec) -> MemorySource;

    /// Constructs a memory source from a vector.
    [[nodiscard]] static auto from_vector(std::vector<std::uint8_t> vec) -> MemorySource;

    /// Constructs a memory source from a vector.
    ///
    /// NOTE: This is non-owning source, so tread carefully!
    [[nodiscard]] static auto from_span(gsl::span<char const> span) -> MemorySource;

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

    /// Constructs a lazy disk-resident memory source from a file.
    ///
    /// \throws NoSuchFile          if the file doesn't exist
    /// \throws std::system_error   if fails to open the file.
    [[nodiscard]] static auto disk_resident_file(std::string const& file) -> MemorySource;

    /// Constructs a lazy disk-resident memory source from a file.
    ///
    /// \throws NoSuchFile          if the file doesn't exist
    /// \throws std::system_error   if fails to open the file.
    [[nodiscard]] static auto disk_resident_file(boost::filesystem::path file) -> MemorySource;

    /// Checks if memory is mapped.
    [[nodiscard]] auto is_mapped() noexcept -> bool;

    /// Number of bytes in the source.
    [[nodiscard]] auto size() const -> size_type;

    /// Subspan of memory.
    ///
    /// \throws std::out_of_range   if offset + size is out of bounds
    [[nodiscard]] auto subspan(size_type offset, size_type size = gsl::dynamic_extent) const
        -> MemorySpan;

    /// Type erasure interface. Any type implementing it are supported as memory source.
    struct Interface {
        Interface() = default;
        Interface(Interface const&) = delete;
        Interface(Interface&&) noexcept = default;
        Interface& operator=(Interface const&) = delete;
        Interface& operator=(Interface&&) noexcept = default;
        virtual ~Interface() = default;

        [[nodiscard]] virtual size_type size() const = 0;

        [[nodiscard]] virtual auto subspan(size_type offset, size_type size) const
            -> MemoryResult = 0;
    };

    /// Actual objects that wrap any type to implement type-earsed interface.
    template <typename T>
    struct Impl: Interface {
        explicit Impl(T source) : m_source(std::move(source)) {}
        Impl() = default;
        Impl(Impl const&) = delete;
        Impl(Impl&& other) noexcept(noexcept(T(std::move(other.m_source)))) = default;
        Impl& operator=(Impl const&) = delete;
        Impl& operator=(Impl&& other) noexcept(noexcept(T(std::move(other.m_source)))) = default;
        ~Impl() = default;

        [[nodiscard]] MemoryResult subspan(size_type offset, size_type size) const override
        {
            return m_source.subspan(offset, size);
        }
        [[nodiscard]] size_type size() const override { return m_source.size(); }

      private:
        T m_source;
    };

    friend class MemorySpan;

  private:
    template <typename T>
    explicit MemorySource(T source) : m_source(std::make_unique<Impl<T>>(std::move(source)))
    {}

    std::shared_ptr<Interface> m_source;
};

class MemorySpan {
    using value_type = char;
    using pointer = value_type const*;
    using size_type = typename mio::mmap_source::size_type;

  public:
    MemorySpan() = default;

    /// Checks if the span owns its memory.
    [[nodiscard]] auto is_owning() noexcept -> bool;

    /// Pointer to the first byte.
    [[nodiscard]] auto data() const -> pointer;

    /// Pointer to the first byte.
    [[nodiscard]] auto begin() const -> pointer;

    /// Pointer to the address after the last byte.
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

    friend class MemorySource;

  private:
    MemorySpan(
        std::shared_ptr<typename MemorySource::Interface> source,
        gsl::span<value_type const> memory_span,
        std::shared_ptr<std::vector<value_type>> owning_memory = nullptr);

    std::shared_ptr<typename MemorySource::Interface> m_source;
    gsl::span<value_type const> m_memory_span;
    std::shared_ptr<std::vector<value_type>> m_owning_memory{};
};

template <class To, class From>
typename std::enable_if_t<
    sizeof(To) == sizeof(From) && std::is_trivially_copyable_v<From> && std::is_trivially_copyable_v<To>,
    To>
bit_cast(const From& src) noexcept
{
    static_assert(
        std::is_trivially_constructible_v<To>,
        "This implementation additionally requires destination type to be trivially constructible");

    To dst;
    std::memcpy(&dst, &src, sizeof(To));
    return dst;
}

template <class To>
typename std::enable_if_t<std::is_trivially_copyable_v<To>, To>
bit_cast(const gsl::span<char const>& src)
{
    if (src.size_bytes() != sizeof(To)) {
        throw std::invalid_argument("When bit-casting from span, it the byte sizes must match.");
    }
    To dst;
    std::memcpy(&dst, src.data(), sizeof(To));
    return dst;
}

}  // namespace pisa
