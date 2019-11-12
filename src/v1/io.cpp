#include "v1/io.hpp"

namespace pisa::v1 {

[[nodiscard]] auto load_bytes(std::string const &data_file) -> std::vector<std::byte>
{
    std::vector<std::byte> data;
    std::basic_ifstream<char> in(data_file.c_str(), std::ios::binary);
    in.seekg(0, std::ios::end);
    std::streamsize size = in.tellg();
    in.seekg(0, std::ios::beg);
    data.resize(size);
    if (not in.read(reinterpret_cast<char *>(data.data()), size)) {
        throw std::runtime_error("Failed reading " + data_file);
    }
    return data;
}

} // namespace pisa::v1
