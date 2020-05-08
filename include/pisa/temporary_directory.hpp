#pragma once

#include <iostream>

#include "boost/filesystem.hpp"

namespace pisa {

class TemporaryDirectory {
  public:
    explicit TemporaryDirectory(bool silent = false);
    TemporaryDirectory(TemporaryDirectory const&) = delete;
    TemporaryDirectory& operator=(TemporaryDirectory const&) = delete;
    TemporaryDirectory(TemporaryDirectory&&) noexcept;
    TemporaryDirectory& operator=(TemporaryDirectory&&) noexcept;
    ~TemporaryDirectory();

    [[nodiscard]] auto path() -> boost::filesystem::path const&;

  private:
    boost::filesystem::path m_dir;
    bool m_silent;
};

}  // namespace pisa
