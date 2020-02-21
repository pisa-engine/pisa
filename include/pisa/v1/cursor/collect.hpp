#pragma once

#include <vector>

namespace pisa::v1 {

template <typename Cursor, typename Transform>
auto collect(Cursor &&cursor, Transform transform)
{
    std::vector<std::decay_t<decltype(transform(cursor))>> vec;
    while (not cursor.empty()) {
        vec.push_back(transform(cursor));
        cursor.advance();
    }
    return vec;
}

template <typename Cursor>
auto collect(Cursor &&cursor)
{
    return collect(std::forward<Cursor>(cursor), [](auto &&cursor) { return *cursor; });
}

template <typename Cursor>
auto collect_with_payload(Cursor &&cursor)
{
    return collect(std::forward<Cursor>(cursor),
                   [](auto &&cursor) { return std::make_pair(*cursor, cursor.payload()); });
}

template <typename Cursor>
auto collect_payloads(Cursor &&cursor)
{
    return collect(std::forward<Cursor>(cursor), [](auto &&cursor) { return cursor.payload(); });
}

} // namespace pisa::v1
