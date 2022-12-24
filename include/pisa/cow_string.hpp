#pragma once

#include <string>
#include <string_view>
#include <variant>

namespace pisa {

/**
 * Copy-on-write string type.
 */
class CowString {
    std::variant<std::string_view, std::string> m_value;

  public:
    explicit CowString(std::string_view value);
    explicit CowString(std::string value);
    CowString(CowString const&);
    CowString(CowString&&);
    CowString& operator=(CowString const&);
    CowString& operator=(CowString&&);
    ~CowString();

    /**
     * Returns a view over the value.
     */
    [[nodiscard]] auto as_view() const -> std::string_view;

    /**
     * Returns an owned string.
     *
     * If the value is borrowed, a string is constructed. If the value is owned,
     * the string is _moved_.
     */
    [[nodiscard]] auto to_owned() && -> std::string;
};

}  // namespace pisa
