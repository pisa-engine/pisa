#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include <Porter2/Porter2.hpp>

#include "io.hpp"
#include "query/queries.hpp"

namespace pisa {

std::function<term_id_type(std::string &&)> term_processor(std::optional<std::string> terms_file,
                                                           bool stem)
{
    if (terms_file) {
        auto to_id = [m = std::make_shared<std::unordered_map<std::string, term_id_type>>(
                          io::read_string_map<term_id_type>(terms_file.value()))](auto str) {
            return m->at(str);
        };
        if (stem) {
            return [=](auto str) {
                stem::Porter2 stemmer{};
                return to_id(stemmer.stem(str));
            };
        } else {
            return to_id;
        }
    } else {
        return [](auto str) { return std::stoi(str); };
    }
}

} // namespace pisa
