#pragma once

#include "boost/filesystem.hpp"

namespace pisa {

[[nodiscard]] auto ls(boost::filesystem::path dir, std::function<bool(std::string const&)> predicate)
{
    std::vector<boost::filesystem::path> files;
    for (auto it = boost::filesystem::directory_iterator(dir);
         it != boost::filesystem::directory_iterator{};
         ++it) {
        if (predicate(it->path().string())) {
            files.push_back(*it);
        }
    }
    return files;
}

}  // namespace pisa
