#pragma once

#include <filesystem>

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
    [[nodiscard]] auto path() -> std::filesystem::path const&;

  private:
    std::filesystem::path dir_;
};

};  // namespace pisa
