#pragma once

namespace pisa::io {

template <typename Integral>
std::unordered_map<std::string, Integral> read_string_map(std::string const &filename)
{
    std::unordered_map<std::string, Integral> mapping;
    std::ifstream is(filename);
    std::string line;
    Integral idx{};
    while (std::getline(is, line)) {
        mapping[line] = idx++;
    }
    return mapping;
}

} // namespace pisa::io
