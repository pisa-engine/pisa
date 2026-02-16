// Copyright 2026 PISA Developers
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

#include <concepts>
#include <cstdint>
#include <memory>
#include <ostream>
#include <sstream>

namespace pisa {

namespace detail {
    class StatsInterface {
      public:
        virtual ~StatsInterface() = default;
        virtual void add(std::string const& key, bool value) = 0;
        virtual void add(std::string const& key, std::int64_t value) = 0;
        virtual void add(std::string const& key, std::uint64_t value) = 0;
        virtual void add(std::string const& key, double value) = 0;
        virtual void add(std::string const& key, const char* value) = 0;
        virtual void add(std::string const& key, std::string value) = 0;
        [[nodiscard]] virtual auto str() const -> std::string = 0;
    };
}  // namespace detail

template <typename T>
concept Streamable = requires(std::ostream& os, T value) {
    { os << value } -> std::convertible_to<std::ostream&>;
};

/**
 * A simple key-value JSON for printing statistics.
 */
class JsonStats {
  private:
    std::unique_ptr<::pisa::detail::StatsInterface> m_impl;

    explicit JsonStats(std::unique_ptr<::pisa::detail::StatsInterface> impl);

  public:
    JsonStats();
    auto add(std::string const& key, bool value) -> JsonStats&;
    auto add(std::string const& key, int value) -> JsonStats&;
    auto add(std::string const& key, unsigned int value) -> JsonStats&;
    auto add(std::string const& key, long value) -> JsonStats&;
    auto add(std::string const& key, unsigned long value) -> JsonStats&;
    auto add(std::string const& key, double value) -> JsonStats&;
    auto add(std::string const& key, const char* value) -> JsonStats&;
    auto add(std::string const& key, std::string value) -> JsonStats&;
    auto add(std::string const& key, std::string_view value) -> JsonStats&;

    template <typename T>
        requires Streamable<T>
    auto add(std::string const& key, T const& value) -> JsonStats& {
        std::ostringstream out;
        out << value;
        add(key, out.str());
        return *this;
    }

    /** Returns the JSON string. */
    [[nodiscard]] auto str() const -> std::string;
};

[[nodiscard]] auto json_stats() -> JsonStats;

}  // namespace pisa
