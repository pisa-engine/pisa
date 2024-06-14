#include "pisa/temporary_directory.hpp"

#include <algorithm>
#include <random>

#include <spdlog/spdlog.h>

namespace pisa {

auto random_name(std::size_t length = 64UL) -> std::string {
    thread_local std::random_device rd{};
    thread_local std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib('a', 'z');
    std::string name;
    std::generate_n(std::back_inserter(name), length, [&]() { return distrib(gen); });
    return name;
}

TemporaryDirectory::TemporaryDirectory()
    : pisa::TemporaryDirectory(std::filesystem::temp_directory_path()) {}

TemporaryDirectory::TemporaryDirectory(std::filesystem::path const& root)
    : dir_(root / random_name()) {
    std::filesystem::create_directory(dir_);
    spdlog::debug("Created a tmp dir {}", dir_.c_str());
}

TemporaryDirectory::TemporaryDirectory(TemporaryDirectory const&) = default;
TemporaryDirectory::TemporaryDirectory(TemporaryDirectory&&) noexcept = default;
TemporaryDirectory& TemporaryDirectory::operator=(TemporaryDirectory const&) = default;
TemporaryDirectory& TemporaryDirectory::operator=(TemporaryDirectory&&) noexcept = default;

TemporaryDirectory::~TemporaryDirectory() {
    if (std::filesystem::exists(dir_)) {
        std::filesystem::remove_all(dir_);
    }
    spdlog::debug("Removed a tmp dir {}", dir_.c_str());
}

auto TemporaryDirectory::path() -> std::filesystem::path const& {
    return dir_;
}

};  // namespace pisa
