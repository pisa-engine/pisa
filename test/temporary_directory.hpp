#pragma once

#include "boost/filesystem.hpp"

struct Temporary_Directory {
    explicit Temporary_Directory()
        : dir_(boost::filesystem::temp_directory_path() / boost::filesystem::unique_path())
    {
        if (boost::filesystem::exists(dir_)) {
            boost::filesystem::remove_all(dir_);
        }
        boost::filesystem::create_directory(dir_);
        std::cerr << "Created a tmp dir " << dir_.c_str() << '\n';
    }
    ~Temporary_Directory() {
        if (boost::filesystem::exists(dir_)) {
            boost::filesystem::remove_all(dir_);
        }
    }

    [[nodiscard]] auto path() -> boost::filesystem::path const & { return dir_; }

   private:
    boost::filesystem::path dir_;
};
