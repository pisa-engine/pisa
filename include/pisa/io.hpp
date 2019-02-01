#pragma once

#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

namespace pisa::io {

template <typename Integral>
[[nodiscard]] inline auto read_string_map(std::string const &filename)
    -> std::unordered_map<std::string, Integral>
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

[[nodiscard]] inline auto read_string_vector(std::string const &filename)
    -> std::vector<std::string>
{
    std::vector<std::string> vec;
    std::ifstream is(filename);
    std::string line;
    while (std::getline(is, line)) {
        vec.push_back(std::move(line));
    }
    return vec;
}

template <typename Function>
void for_each_line(std::istream &is, Function fn)
{
    std::string line;
    while (std::getline(is, line)) {
        fn(line);
    }
}

} // namespace pisa::io
