#pragma once

#include <string>
#include <string_view>

namespace pisa {

class TextFilter {
  public:
    TextFilter();
    TextFilter(TextFilter const&);
    TextFilter(TextFilter&&);
    TextFilter& operator=(TextFilter const&);
    TextFilter& operator=(TextFilter&&);
    virtual ~TextFilter();

    [[nodiscard]] virtual auto filter(std::string_view input) -> std::string = 0;
};

class StripHtmlFilter final: public TextFilter {
  public:
    [[nodiscard]] auto filter(std::string_view input) -> std::string override;
};

}  // namespace pisa
