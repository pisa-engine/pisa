#pragma once

#include <algorithm>
#include <string>
#include <utility>

namespace pisa {
namespace util {
std::string ltrim(const std::string &&str) {
    auto trimmed = str;
    auto it2     = std::find_if(trimmed.begin(), trimmed.end(), [](char ch) {
        return !std::isspace<char>(ch, std::locale::classic());
    });
    trimmed.erase(trimmed.begin(), it2);
    return trimmed;
}

std::string rtrim(const std::string &&str) {
    auto trimmed = str;
    auto it1     = std::find_if(trimmed.rbegin(), trimmed.rend(), [](char ch) {
        return !std::isspace<char>(ch, std::locale::classic());
    });
    trimmed.erase(it1.base(), trimmed.end());
    return trimmed;
}
std::string trim(const std::string &&str) { return ltrim(rtrim(std::forward<decltype(str)>(str))); }

}  // namespace util
}  // namespace pisa