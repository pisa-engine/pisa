// Copyright 2024 PISA developers
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <exception>
#include <iostream>
#include <string>

namespace pisa {

class FileOpenError: public std::exception {
  public:
    explicit FileOpenError(std::string const& file);
    [[nodiscard]] auto what() const noexcept -> char const*;

  private:
    std::string m_message;
};

class WriteError: public std::exception {
  public:
    [[nodiscard]] auto what() const noexcept -> char const*;
};

auto open_file_w(std::string const& filename) -> std::ofstream;

template <typename CharT>
auto put(std::basic_ostream<CharT>& stream, CharT ch) -> std::ostream& {
    if (!stream.put(ch)) {
        throw WriteError();
    }
    return stream;
}

template <typename CharT>
auto write(std::basic_ostream<CharT>& stream, CharT const* data, std::streamsize count)
    -> std::basic_ostream<CharT>& {
    if (!stream.write(data, count)) {
        throw WriteError();
    }
    return stream;
}

}  // namespace pisa
