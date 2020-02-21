#pragma once

#include <cstddef>
#include <fstream>
#include <string>
#include <vector>

#include <fmt/format.h>

#include <v1/runtime_assert.hpp>

namespace pisa::v1 {

[[nodiscard]] auto load_bytes(std::string const& data_file) -> std::vector<std::byte>;

template <typename T>
[[nodiscard]] auto load_vector(std::string const& data_file) -> std::vector<T>
{
    std::vector<T> data;
    std::basic_ifstream<char> in(data_file.c_str(), std::ios::binary);
    in.seekg(0, std::ios::end);
    std::streamsize size = in.tellg();
    in.seekg(0, std::ios::beg);

    runtime_assert(size % sizeof(T) == 0).or_exit([&] {
        return fmt::format("Tried loading a vector of elements of size {} but size of file is {}",
                           sizeof(T),
                           size);
    });
    data.resize(size / sizeof(T));

    runtime_assert(in.read(reinterpret_cast<char*>(data.data()), size).good()).or_exit([&] {
        return fmt::format("Failed reading ", data_file);
    });
    return data;
}

} // namespace pisa::v1
