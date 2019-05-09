#include <boost/filesystem.hpp>

#include "payload_vector.hpp"

namespace pisa {

namespace detail {
    struct test {
        static_assert(sizeofs<std::byte>::value == 1);
        static_assert(sizeofs<uint32_t>::value == 4);
        static_assert(sizeofs<std::byte, uint32_t>::value == 5);
    };
} // namespace detail

[[nodiscard]] auto Payload_Vector_Buffer::from_file(std::string const &filename)
    -> Payload_Vector_Buffer
{
    boost::system::error_code ec;
    auto file_size = boost::filesystem::file_size(boost::filesystem::path(filename));
    std::ifstream is(filename);

    size_type len;
    is.read(reinterpret_cast<char *>(&len), sizeof(size_type));

    auto offsets_bytes = (len + 1) * sizeof(size_type);
    std::vector<size_type> offsets(len + 1);
    is.read(reinterpret_cast<char *>(offsets.data()), offsets_bytes);

    auto payloads_bytes = file_size - offsets_bytes - sizeof(size_type);
    std::vector<std::byte> payloads(payloads_bytes);
    is.read(reinterpret_cast<char *>(payloads.data()), payloads_bytes);

    return Payload_Vector_Buffer{std::move(offsets), std::move(payloads)};
}

void Payload_Vector_Buffer::to_file(std::string const &filename) const
{
    std::ofstream is(filename);
    to_stream(is);
}

void Payload_Vector_Buffer::to_stream(std::ostream &is) const
{
    size_type length = offsets.size() - 1u;
    is.write(reinterpret_cast<char const *>(&length), sizeof(length));
    is.write(reinterpret_cast<char const *>(offsets.data()), offsets.size() * sizeof(offsets[0]));
    is.write(reinterpret_cast<char const *>(payloads.data()), payloads.size());
}

[[nodiscard]] auto encode_payload_vector(gsl::span<std::string const> values)
    -> Payload_Vector_Buffer
{
    return encode_payload_vector(values.begin(), values.end());
}

[[nodiscard]] auto split(gsl::span<std::byte const> mem, std::size_t offset)
    -> std::tuple<gsl::span<std::byte const>, gsl::span<std::byte const>>
{
    if (offset > mem.size()) {
        throw std::runtime_error(
            fmt::format("Cannot split span of size {} at position {}", mem.size(), offset));
    }
    return std::tuple(mem.first(offset), mem.subspan(offset));
}

} // namespace pisa
