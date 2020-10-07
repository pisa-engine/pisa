#pragma once

#include "boost/algorithm/string/split.hpp"
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <fstream>
#include <optional>
#include <sstream>
#include <string>

#include <iostream>

namespace pisa {

class aol_reader {
  public:
    explicit aol_reader(std::istream& is) : m_is(is) {}

    std::optional<std::string> next_query()
    {
        m_is >> std::ws;
        while (not m_is.eof()) {
            std::string line;
            std::getline(m_is, line);
            std::vector<std::string> fields;
            boost::algorithm::split(fields, line, boost::is_any_of("\t"));
            if (fields.size() > 3 and fields[1].empty() and fields[1] != "-") {
                return std::make_optional(fields[1]);
            }
        }
        return std::nullopt;
    }

  private:
    std::istream& m_is;
};

}  // namespace pisa
