#pragma once

#include <string>
#include "gumbo.h"

namespace pisa::parsing::html {

[[nodiscard]] inline auto cleantext(GumboNode *node) -> std::string
{
    if (node->type == GUMBO_NODE_TEXT) {
        return std::string(node->v.text.text); // NOLINT
    }
    if (node->type == GUMBO_NODE_ELEMENT && node->v.element.tag != GUMBO_TAG_SCRIPT && // NOLINT
        node->v.element.tag != GUMBO_TAG_STYLE) // NOLINT
    {
        std::string contents;
        GumboVector *children = &node->v.element.children; // NOLINT
        for (unsigned int i = 0; i < children->length; ++i) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            std::string const text = cleantext(reinterpret_cast<GumboNode *>(children->data[i]));
            if (i != 0 && not contents.empty() && not text.empty()) {
                contents.append(" ");
            }
            contents.append(text);
        }
        return contents;
    }
    return std::string();
}

[[nodiscard]] inline auto cleantext(std::string const &html) -> std::string
{
    GumboOutput *output  = gumbo_parse(html.c_str());
    std::string  content = cleantext(output->root);
    gumbo_destroy_output(&kGumboDefaultOptions, output);
    return content;
}

} // namespace pisa::parsing::html
