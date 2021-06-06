#pragma once

#include <istream>
#include <string>
#include <utility>

namespace pisa {

struct Document_Record {
    Document_Record(std::string title, std::string content, std::string url)
        : title_(std::move(title)), content_(std::move(content)), url_(std::move(url))
    {}
    [[nodiscard]] auto title() noexcept -> std::string& { return title_; }
    [[nodiscard]] auto title() const noexcept -> std::string const& { return title_; }
    [[nodiscard]] auto content() noexcept -> std::string& { return content_; }
    [[nodiscard]] auto content() const noexcept -> std::string const& { return content_; }
    [[nodiscard]] auto url() noexcept -> std::string& { return url_; }
    [[nodiscard]] auto url() const noexcept -> std::string const& { return url_; }

  private:
    std::string title_;
    std::string content_;
    std::string url_;
};

class Plaintext_Record {
  public:
    Plaintext_Record() = default;
    Plaintext_Record(std::string trecid, std::string content)
        : m_trecid(std::move(trecid)), m_content(std::move(content))
    {}
    [[nodiscard]] auto content() -> std::string& { return m_content; }
    [[nodiscard]] auto content() const -> std::string const& { return m_content; }
    [[nodiscard]] auto trecid() -> std::string& { return m_trecid; }
    [[nodiscard]] auto trecid() const -> std::string const& { return m_trecid; }
    [[nodiscard]] auto url() -> std::string& { return m_url; }
    [[nodiscard]] auto url() const -> std::string const& { return m_url; }
    [[nodiscard]] auto valid() const noexcept -> bool { return true; }
    [[nodiscard]] static auto read(std::istream& is) {}

  private:
    std::string m_trecid;
    std::string m_content;
    std::string m_url;
};

}  // namespace pisa

inline auto operator>>(std::istream& is, pisa::Plaintext_Record& record) -> std::istream&
{
    is >> record.trecid();
    std::getline(is, record.content());
    return is;
}
