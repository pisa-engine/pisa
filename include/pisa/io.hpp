#pragma once

#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/filesystem.hpp>
#include <fmt/format.h>

namespace pisa::io {

class NoSuchFile {
  public:
    explicit NoSuchFile(std::string file) : m_file(std::move(file)) {}
    [[nodiscard]] auto what() const -> std::string {
        return fmt::format("No such file: {}", m_file);
    }

  private:
    std::string m_file;
};

[[nodiscard]] inline auto resolve_path(std::string const& file) -> boost::filesystem::path
{
    boost::filesystem::path p(file);
    if (not boost::filesystem::exists(p)) {
        throw NoSuchFile(file);
    }
    return p;
}

class Line: public std::string {
    friend std::istream& operator>>(std::istream& is, Line& line) { return std::getline(is, line); }
};

template <typename Integral>
[[nodiscard]] inline auto read_string_map(std::string const& filename)
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

[[nodiscard]] inline auto read_string_vector(std::string const& filename) -> std::vector<std::string>
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
void for_each_line(std::istream& is, Function fn)
{
    std::string line;
    while (std::getline(is, line)) {
        fn(line);
    }
}

[[nodiscard]] inline auto load_data(std::string const& data_file)
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

}  // namespace pisa::io
