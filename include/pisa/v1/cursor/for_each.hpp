#pragma once

#include <vector>

namespace pisa::v1 {

template <typename Cursor, typename UnaryOp>
void for_each(Cursor &&cursor, UnaryOp op)
{
    while (not cursor.empty()) {
        op(cursor);
        cursor.step();
    }
}

} // namespace pisa::v1
