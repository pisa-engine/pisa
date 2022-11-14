#pragma once

#include <boost/filesystem.hpp>

namespace pisa {

/**
 * RAII object that creates a temporary directory at creation and removes it when destructed.
 */
struct TemporaryDirectory {
    TemporaryDirectory();
    TemporaryDirectory(TemporaryDirectory const&);
    TemporaryDirectory(TemporaryDirectory&&) noexcept;
    TemporaryDirectory& operator=(TemporaryDirectory const&);
    TemporaryDirectory& operator=(TemporaryDirectory&&) noexcept;
    ~TemporaryDirectory();

    /** Returns the path to the created directory. */
    [[nodiscard]] auto path() -> boost::filesystem::path const&;

  private:
    boost::filesystem::path dir_;
};

};  // namespace pisa
