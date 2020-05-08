#include "temporary_directory.hpp"

namespace pisa {

TemporaryDirectory::TemporaryDirectory(bool silent)
    : m_dir(boost::filesystem::temp_directory_path() / boost::filesystem::unique_path()),
      m_silent(silent)
{
    if (boost::filesystem::exists(m_dir)) {
        boost::filesystem::remove_all(m_dir);
    }
    boost::filesystem::create_directory(m_dir);
    if (not silent) {
        std::cerr << "Created a tmp dir " << m_dir.c_str() << '\n';
    }
}

TemporaryDirectory::TemporaryDirectory(TemporaryDirectory&&) noexcept = default;

TemporaryDirectory& TemporaryDirectory::operator=(TemporaryDirectory&&) noexcept = default;

TemporaryDirectory::~TemporaryDirectory()
{
    if (boost::filesystem::exists(m_dir)) {
        boost::filesystem::remove_all(m_dir);
    }
    if (not m_dir.empty() && not m_silent) {
        std::cerr << "Removed a tmp dir " << m_dir.c_str() << '\n';
    }
}

auto TemporaryDirectory::path() -> boost::filesystem::path const&
{
    return m_dir;
}

}  // namespace pisa
