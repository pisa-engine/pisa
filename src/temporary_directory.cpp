#include "pisa/temporary_directory.hpp"

#include <iostream>

namespace pisa {

TemporaryDirectory::TemporaryDirectory()
    : dir_(boost::filesystem::temp_directory_path() / boost::filesystem::unique_path())
{
    if (boost::filesystem::exists(dir_)) {
        boost::filesystem::remove_all(dir_);
    }
    boost::filesystem::create_directory(dir_);
    std::cerr << "Created a tmp dir " << dir_.c_str() << '\n';
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
    std::cerr << "Removed a tmp dir " << dir_.c_str() << '\n';
}

auto TemporaryDirectory::path() -> boost::filesystem::path const&
{
    return dir_;
}

};  // namespace pisa
