#pragma once

#include <functional>
#include <istream>
#include <optional>

#include "document_record.hpp"

namespace pisa {

void parse_plaintext_content(std::string&& content, std::function<void(std::string&&)> process);
void parse_html_content(std::string&& content, std::function<void(std::string&&)> process);

std::function<std::optional<Document_Record>(std::istream&)>
record_parser(std::string const& type, std::istream& is);

std::function<void(std::string&& constent, std::function<void(std::string&&)>)>
content_parser(std::optional<std::string> const& type);

}  // namespace pisa
