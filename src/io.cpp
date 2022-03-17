#include <fmt/format.h>

#include "io.hpp"

namespace pisa::io {

NoSuchFile::NoSuchFile(std::string const& file) : m_message(fmt::format("No such file: {}", file))
{}

[[nodiscard]] auto NoSuchFile::what() const noexcept -> char const*
{
    return m_message.c_str();
}

auto resolve_path(std::string const& file) -> boost::filesystem::path
{
    boost::filesystem::path p(file);
    if (not boost::filesystem::exists(p)) {
        throw NoSuchFile(file);
    }
    return p;
}

auto read_string_vector(std::string const& filename) -> std::vector<std::string>
{
    std::vector<std::string> vec;
    std::ifstream is(filename);
    std::string line;
    while (std::getline(is, line)) {
        vec.push_back(std::move(line));
    }
    return vec;
}

auto load_data(std::string const& data_file) -> std::vector<char>
{
    std::vector<char> data;
    std::ifstream in(data_file.c_str(), std::ios::binary);
    in.seekg(0, std::ios::end);
    std::streamsize size = in.tellg();
    in.seekg(0, std::ios::beg);
    data.resize(size);
    if (not in.read(data.data(), size)) {
        throw std::runtime_error("Failed reading " + data_file);
    }
    return data;
}

void write_data(std::string const& data_file, gsl::span<std::byte const> bytes)
{
    std::ofstream os(data_file);
    os.write(reinterpret_cast<char const*>(bytes.data()), bytes.size());
}

}  // namespace pisa::io
