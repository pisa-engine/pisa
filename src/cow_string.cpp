#include "pisa/cow_string.hpp"

#include <iostream>

namespace pisa {

CowString::CowString(std::string_view value) : m_value(value) {}

CowString::CowString(std::string value) : m_value(std::move(value)) {}

CowString::CowString(CowString const&) = default;
CowString::CowString(CowString&&) = default;
CowString& CowString::operator=(CowString const&) = default;
CowString& CowString::operator=(CowString&&) = default;
CowString::~CowString() = default;

auto CowString::as_view() const -> std::string_view {
    if (auto* value = std::get_if<std::string_view>(&m_value); value != nullptr) {
        return *value;
    }
    return std::string_view(*std::get_if<std::string>(&m_value));
}

auto CowString::to_owned() && -> std::string {
    if (auto* value = std::get_if<std::string_view>(&m_value); value != nullptr) {
        return std::string(*value);
    }
    return std::move(*std::get_if<std::string>(&m_value));
}

}  // namespace pisa
