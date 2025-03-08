#pragma once

#include <filesystem>

namespace pisa {

/**
 * RAII object that creates a temporary directory at creation and removes it when destructed.
 */
struct TemporaryDirectory {
    /** Constructs a directory in the system temp directory, e.g., /tmp on Linux. */
    TemporaryDirectory();

    /** Constructs a directory in the root directory. */
    explicit TemporaryDirectory(std::filesystem::path const& root);

    TemporaryDirectory(TemporaryDirectory const&);
    TemporaryDirectory(TemporaryDirectory&&) noexcept;
    TemporaryDirectory& operator=(TemporaryDirectory const&);
    TemporaryDirectory& operator=(TemporaryDirectory&&) noexcept;
    ~TemporaryDirectory();

    /** Returns the path to the created directory. */
    [[nodiscard]] auto path() const -> std::filesystem::path const&;

    /**
     * Directory will not be cleaned up when the destructor is called.
     * Useful for debugging.
     **/
    void disable_cleanup();

  private:
    std::filesystem::path dir_;
    bool cleanup_ = true;
};

};  // namespace pisa
