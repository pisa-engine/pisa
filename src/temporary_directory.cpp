#include "pisa/temporary_directory.hpp"

#include <iostream>

#include <spdlog/spdlog.h>

namespace pisa {

TemporaryDirectory::TemporaryDirectory()
    : dir_(boost::filesystem::temp_directory_path() / boost::filesystem::unique_path())
{
    if (boost::filesystem::exists(dir_)) {
        boost::filesystem::remove_all(dir_);
    }
    boost::filesystem::create_directory(dir_);
    spdlog::debug("Created a tmp dir {}", dir_.c_str());
}

TemporaryDirectory::TemporaryDirectory(TemporaryDirectory const&) = default;
TemporaryDirectory::TemporaryDirectory(TemporaryDirectory&&) noexcept = default;
TemporaryDirectory& TemporaryDirectory::operator=(TemporaryDirectory const&) = default;
TemporaryDirectory& TemporaryDirectory::operator=(TemporaryDirectory&&) noexcept = default;

TemporaryDirectory::~TemporaryDirectory()
{
    if (boost::filesystem::exists(dir_)) {
        boost::filesystem::remove_all(dir_);
    }
    spdlog::debug("Removed a tmp dir {}", dir_.c_str());
}

auto TemporaryDirectory::path() -> boost::filesystem::path const&
{
    return dir_;
}

};  // namespace pisa
