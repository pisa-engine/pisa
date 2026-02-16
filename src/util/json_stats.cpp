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

#include "pisa/util/json_stats.hpp"
#include "nlohmann/json.hpp"

namespace pisa {

class JsonStatsImpl: public ::pisa::detail::StatsInterface {
  private:
    nlohmann::json m_json;

  public:
    virtual void add(std::string const& key, bool value) { m_json[key] = value; }
    virtual void add(std::string const& key, std::int64_t value) { m_json[key] = value; }
    virtual void add(std::string const& key, std::uint64_t value) { m_json[key] = value; }
    virtual void add(std::string const& key, double value) { m_json[key] = value; }
    virtual void add(std::string const& key, std::string value) { m_json[key] = std::move(value); }
    virtual void add(std::string const& key, const char* value) { m_json[key] = value; }
    [[nodiscard]] virtual auto str() const -> std::string { return m_json.dump(2); }
};

[[nodiscard]] auto json_stats() -> JsonStats {
    return JsonStats();
}
JsonStats::JsonStats() : m_impl(std::make_unique<JsonStatsImpl>()) {}
JsonStats::JsonStats(std::unique_ptr<::pisa::detail::StatsInterface> impl)
    : m_impl(std::move(impl)) {}
[[nodiscard]] auto JsonStats::add(std::string const& key, bool value) -> JsonStats& {
    m_impl->add(key, value);
    return *this;
}
[[nodiscard]] auto JsonStats::add(std::string const& key, int value) -> JsonStats& {
    m_impl->add(key, static_cast<std::int64_t>(value));
    return *this;
}
[[nodiscard]] auto JsonStats::add(std::string const& key, unsigned int value) -> JsonStats& {
    m_impl->add(key, static_cast<std::uint64_t>(value));
    return *this;
}
[[nodiscard]] auto JsonStats::add(std::string const& key, long value) -> JsonStats& {
    m_impl->add(key, static_cast<std::int64_t>(value));
    return *this;
}
[[nodiscard]] auto JsonStats::add(std::string const& key, unsigned long value) -> JsonStats& {
    m_impl->add(key, static_cast<std::uint64_t>(value));
    return *this;
}
[[nodiscard]] auto JsonStats::add(std::string const& key, double value) -> JsonStats& {
    m_impl->add(key, value);
    return *this;
}
[[nodiscard]] auto JsonStats::add(std::string const& key, const char* value) -> JsonStats& {
    m_impl->add(key, value);
    return *this;
}
[[nodiscard]] auto JsonStats::add(std::string const& key, std::string value) -> JsonStats& {
    m_impl->add(key, value);
    return *this;
}
[[nodiscard]] auto JsonStats::add(std::string const& key, std::string_view value) -> JsonStats& {
    m_impl->add(key, std::string(value));
    return *this;
}
[[nodiscard]] auto JsonStats::str() const -> std::string {
    return m_impl->str();
}

}  // namespace pisa
