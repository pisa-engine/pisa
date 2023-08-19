#pragma once

#include <filesystem>
#include <functional>
#include <string>
#include <vector>

namespace pisa {

[[nodiscard]] auto ls(std::filesystem::path dir, std::function<bool(std::string const&)> predicate)
{
    std::vector<std::filesystem::path> files;
    for (auto it = std::filesystem::directory_iterator(dir);
         it != std::filesystem::directory_iterator{};
         ++it) {
        if (predicate(it->path().string())) {
            files.push_back(*it);
        }
    }
    return files;
}

}  // namespace pisa
