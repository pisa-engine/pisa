#include <cstdint>
#include <vector>

namespace {

/// Collect elements of the cursor to an output iterator by applying a transformation
/// to the cursor at each element.
template <typename Cursor, typename Transform>
auto for_each(Cursor &cursor, Transform transform) -> std::size_t
{
    std::size_t len = 0;
    while (cursor.docid() < cursor.sentinel()) {
        transform(cursor);
        ++len;
    }
    return len;
}

/// Collect elements of the cursor to an output iterator by applying a transformation
/// to the cursor at each element.
template <typename Cursor, typename OutputIterator, typename Transform>
auto collect(Cursor &cursor, OutputIterator output, Transform transform) -> std::size_t
{
    std::size_t len = 0;
    while (cursor.docid() < cursor.sentinel()) {
        *output++ = transform(cursor);
        ++len;
    }
    return len;
}

template <typename Cursor, typename Transform>
auto collect_to_vec(Cursor &cursor, Transform transform) -> std::vector<decltype(transform(cursor))>
{
    std::vector<decltype(transform(cursor))> vec;
    collect(cursor, std::back_inserter(vec), transform);
    return vec;
}

} // namespace
