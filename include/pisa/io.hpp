#pragma once

#include <exception>
#include <iostream>
#include <string>

#include <boost/filesystem.hpp>
#include <gsl/span>

namespace pisa::io {

/// Indicates that a file was not found.
///
/// As opposed to the standard c++ IO error, this one preserves the file name in the message for
/// more informative logging.
class NoSuchFile: public std::exception {
  public:
    explicit NoSuchFile(std::string const& file);
    [[nodiscard]] auto what() const noexcept -> char const*;

  private:
    std::string m_message;
};

/// Resolves string as a path; throws NoSuchFile if the file does not exist.
[[nodiscard]] auto resolve_path(std::string const& file) -> boost::filesystem::path;

class Line: public std::string {
    friend std::istream& operator>>(std::istream& is, Line& line) { return std::getline(is, line); }
};

/// Reads a vector of strings from a newline-delimited text file.
[[nodiscard]] auto read_string_vector(std::string const& filename) -> std::vector<std::string>;

template <typename Function>
void for_each_line(std::istream& is, Function fn)
{
    std::string line;
    while (std::getline(is, line)) {
        fn(line);
    }
}

/// Loads bytes from a file.
[[nodiscard]] auto load_data(std::string const& data_file) -> std::vector<char>;

/// Writes bytes to a file.
void write_data(std::string const& data_file, gsl::span<std::byte const> bytes);

}  // namespace pisa::io
